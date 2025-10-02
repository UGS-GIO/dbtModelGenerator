

const { Client } = require('pg');
const fs = require('fs').promises;
const yaml = require('js-yaml');

class MetadataAwareGenerator {
    constructor(dbConfig, dbtProjectPath) {
        this.db = new Client(dbConfig);
        this.dbtProjectPath = dbtProjectPath;
    }

    async generateAllModelsForFirstLoad(baseTableName) {
        console.log(`Generating dbt models for first load: ${baseTableName}`);
        const metadata = await this.getMetadataForTable(baseTableName);
        
        await this.generateSourcesYml(baseTableName, metadata);
        await this.generateStagingModel(baseTableName, metadata);
        await this.generateIntermediateModel(baseTableName, metadata);
        await this.generateMartsModel(baseTableName, metadata);
        
        console.log(`Generated complete dbt pipeline for ${baseTableName}`);
        console.log(`IMPORTANT: These models will automatically handle future schema versions`);
        console.log(`No regeneration needed when v2, v3, etc. arrive`);
    }

    async getMetadataForTable(baseTableName) {
        // Escape LIKE wildcards in the base table name
        const escapedTableName = baseTableName
            .replace(/\\/g, '\\\\')  // Escape backslashes first
            .replace(/%/g, '\\%')    // Escape percent signs  
            .replace(/_/g, '\\_');   // Escape underscores
        
        const result = await this.db.query(`
            SELECT 
                table_name,
                created_at as load_date,
                (metadata->>'publicationDate')::date as publication_date,
                metadata
            FROM raw.metadata 
            WHERE table_name LIKE $1 ESCAPE '\\'
            ORDER BY (metadata->>'publicationDate')::date
        `, [`${escapedTableName}%`]);
        
        return result.rows.map(row => {
            // metadata is already an object, no need to parse
            const metadataJson = row.metadata;
            return {
                tableName: row.table_name,
                loadDate: row.load_date,
                publicationDate: row.publication_date,
                schemaVersion: metadataJson.schema_version,
                reviewStatus: metadataJson.review_status,
                tableType: metadataJson.table_type,
                uniqueKey: metadataJson.unique_key || null,
                sourceColumns: metadataJson.schemaValidation?.sourceColumns || [],
                columnMapping: metadataJson.schemaValidation?.columnMapping || {},
                unifiedViewName: metadataJson.unified_view_name,
                correctionOf: metadataJson.correction_of || null,
                scaleNumber: metadataJson.scale_number || null,
                scaleSize: metadataJson.scale_size || null
            };
        });
    }

    getUnifiedViewSchema(metadata) {
        const allSourceColumns = new Set();
        metadata.forEach(entry => {
            entry.sourceColumns.forEach(col => allSourceColumns.add(col));
        });
        return Array.from(allSourceColumns);
    }

    getBusinessSchema(metadata) {
        const allMappedColumns = new Set();
        const allSourceColumns = new Set();
        
        metadata.forEach(entry => {
            Object.values(entry.columnMapping).forEach(mappedCol => {
                allMappedColumns.add(mappedCol);
            });
            Object.keys(entry.columnMapping).forEach(sourceCol => {
                allSourceColumns.add(sourceCol);
            });
        });
        
        const unifiedSchema = this.getUnifiedViewSchema(metadata);
        unifiedSchema.forEach(sourceCol => {
            if (!allSourceColumns.has(sourceCol)) {
                allMappedColumns.add(sourceCol);
            }
        });
        return Array.from(allMappedColumns);
    }
    
    async generateSourcesYml(baseTableName, metadata) {
        const dataTopic = baseTableName.split('_')[1];
        const unifiedViewSchema = this.getUnifiedViewSchema(metadata);
        const unifiedViewName = metadata[0].unifiedViewName || `${baseTableName}_raw_unified`;
        
        const sourcesConfig = {
            version: 2,
            sources: [{
                name: `${baseTableName}_data`,
                description: `${baseTableName} data from ingestion pipeline`,
                database: 'seamlessgeolmap',
                schema: 'raw',
                tables: [{
                    name: unifiedViewName,
                    description: `${baseTableName}: unified view with original source column names`,
                    freshness: {
                        warn_after: { count: 24, period: 'hour' },
                        error_after: { count: 48, period: 'hour' }
                    },
                    
                    columns: [
                        ...unifiedViewSchema.map(col => ({
                            name: col.toLowerCase(),
                            description: `${col} field (original source column name)`
                        })),
                        {
                            name: '_schema_version',
                            description: 'Schema version (v1, v2, etc.) - links to raw.metadata table'
                        },
                        {
                            name: '_publication_id', 
                            description: 'Internal publication ID extracted from table name'
                        },
                        {
                            name: '_quad_name',
                            description: 'Quadrangle name for spatial datasets (may be null for non-spatial data)'
                        },
                        {
                            name: '_publication_date',
                            description: 'Business publication date for proper chronological ordering'
                        },
                        {
                            name: '_load_date',
                            description: 'Date when data was loaded into the system'
                        },
                        {
                            name: '_source_table',
                            description: 'Original timestamped raw table name for lineage tracking'
                        }
                    ]
                }]
            }]
        };
        const sourcesPath = `${this.dbtProjectPath}/models/staging/sources.yml`;
        await this.writeYamlFile(sourcesPath, sourcesConfig);
        console.log(`Generated: sources.yml`);
    }

    generateColumnMappings(metadata) {
        const businessColumns = this.getBusinessSchema(metadata);
        const baseTableName = this.extractBaseTableName(metadata[0].tableName);
        
        return businessColumns.map(businessCol => {
            const isMapped = this.isColumnMapped(businessCol, metadata);
            
            if (isMapped) {
                return `        {{ map_source_column_to_business('${businessCol}', '${baseTableName}') }} as ${businessCol}`;
            } else {
                return `        {{ map_unmapped_column('${businessCol}', '${baseTableName}') }} as ${businessCol}`;
            }
        }).join(',\n') + ',';
    }
    
    isColumnMapped(businessColumn, metadata) {
        for (const entry of metadata) {
            if (Object.values(entry.columnMapping).includes(businessColumn)) {
                return true;
            }
        }
        return false;
    }
    
    extractBaseTableName(fullTableName) {
        const parts = fullTableName.split('_');
        if (parts.length >= 2) {
            return `${parts[0]}_${parts[1]}`;
        }
        return fullTableName;
    }

    async generateStagingModel(baseTableName, metadata) {
        const unifiedViewName = metadata[0].unifiedViewName || `${baseTableName}_raw_unified`;
        const baseTableNameForMacro = this.extractBaseTableName(metadata[0].tableName);
        
        const stagingSQL = `-- models/staging/stg_${baseTableName}.sql
            -- This model performs two key tasks:
            -- 1. Dynamically maps varied source columns to stable business column names.
            -- 2. Enriches the data by joining table-level metadata to every row.

            {{ config(materialized='view') }}

            WITH raw_data AS (
                SELECT * FROM {{ source('${baseTableName}_data', '${unifiedViewName}') }}
            ),
            
            metadata_enrichment AS (
                -- This CTE fetches all enrichment fields from the metadata table.
                SELECT
                    table_name,
                    (metadata->>'publication_id')::text as metadata_publication_id,
                    (metadata->>'scale_number')::integer as scale_number,
                    (metadata->>'scale_size')::text as scale_size,
                    (metadata->>'table_type')::text as table_type,
                    (metadata->>'review_status')::text as review_status
                FROM raw.metadata
                WHERE table_name LIKE '${baseTableNameForMacro}%'
            ),

            mapped_columns AS (
                SELECT 
                    ${this.generateColumnMappings(metadata)}
                    -- Pass through lineage fields from the unified view.
                    raw._schema_version,
                    raw._publication_id,
                    raw._quad_name,
                    raw._publication_date,
                    raw._load_date,
                    raw._source_table,
                    
                    -- Add the new enriched fields from the metadata table.
                    meta.metadata_publication_id,
                    meta.scale_number,
                    meta.scale_size,
                    meta.table_type,
                    meta.review_status
                FROM raw_data raw
                LEFT JOIN metadata_enrichment meta ON raw._source_table = meta.table_name
            )

            SELECT
                *,
                CURRENT_TIMESTAMP as processed_at
            FROM mapped_columns
            WHERE ${this.generateFilterConditions(metadata)}`;

        await fs.writeFile(`${this.dbtProjectPath}/models/staging/stg_${baseTableName}.sql`, stagingSQL);
        console.log(`Generated: stg_${baseTableName}.sql`);
    }

    generateFilterConditions(metadata) {
        const primaryKey = metadata[0].uniqueKey;
        if (!primaryKey) {
            throw new Error(`No unique_key defined in metadata. Cannot generate filter conditions.`);
        }
        return `${primaryKey} IS NOT NULL`;
    }

    async generateIntermediateModel(baseTableName, metadata) {
        const businessColumns = this.getBusinessSchema(metadata);
        const intermediateSQL = `-- models/intermediate/int_${baseTableName}_reprojected.sql
            -- This model handles spatial transformations and validation.

            {{ config(materialized='view') }}

            WITH staging_data AS (
                SELECT * FROM {{ ref('stg_${baseTableName}') }}
            ),
    
            spatial_enhanced AS (
                SELECT 
                    -- Pass through all business columns except for the original geometry.
                    ${businessColumns.filter(col => !col.includes('geometry')).join(',\n        ')},
                    
                    -- Reproject geometry to Web Mercator (EPSG:3857) for web mapping.
                    ST_Transform(geometry, 3857) as geometry,
                    
                    ${this.generateSpatialDerivedFields()}
                    
                    -- Add projection info as metadata.
                    ST_SRID(geometry) as original_srid,
                    3857 as target_srid,
                    'WGS 84 / Pseudo-Mercator' as projection_name,
        
                    -- Pass through all enriched and metadata fields from staging.
                    publication_id,
                    scale_number,
                    scale_size,
                    table_type,
                    review_status,
                    _schema_version,
                    _publication_id,
                    _quad_name,
                    _publication_date,
                    _load_date,
                    _source_table,
                    processed_at
                FROM staging_data
                WHERE geometry IS NOT NULL
            )

            SELECT * FROM spatial_enhanced`;
        await fs.writeFile(`${this.dbtProjectPath}/models/intermediate/int_${baseTableName}_reprojected.sql`, intermediateSQL);
        console.log(`Generated: int_${baseTableName}_reprojected.sql`);
    }

    generateSpatialDerivedFields() {
        return `        -- Geometry validation
        ST_IsValid(geometry) as geometry_is_valid,
        ST_GeometryType(geometry) as geometry_type,`;
    }

    async generateMartsModel(baseTableName, metadata) {
        const primaryKey = metadata[0].uniqueKey;
        
        if (!primaryKey) {
            throw new Error(`No unique_key defined in metadata for ${baseTableName}. This must be set during first ingestion.`);
        }
        
        const martsSQL = `-- models/marts/mart_${baseTableName}.sql
            -- This model applies Slowly Changing Dimension (SCD) Type 2 logic
            -- to track the history of each record over time.

            {{ config(materialized='view') }}

            WITH intermediate_data AS (
                SELECT * FROM {{ ref('int_${baseTableName}_reprojected') }}
            ),

            scd_enhanced AS (
                SELECT 
                    -- Select all columns from the intermediate model.
                    *,
                    -- Generate the SCD validity fields (valid_from, valid_to, is_current).
                    ${this.generateSCDFields(primaryKey)}
                FROM intermediate_data
            ),

            final AS (
                SELECT 
                    *,
                    -- Classify each record as an INSERT or UPDATE.
                    ${this.generateChangeTypeLogic(primaryKey)}
                    
                    -- Create a user-friendly status field.
                    CASE 
                        WHEN is_current = 'R' THEN 'PENDING_REVIEW'
                        WHEN is_current = 'Y' THEN 'CURRENT'
                        WHEN is_current = 'N' THEN 'HISTORICAL'
                    END as record_status,
                    
                    CURRENT_TIMESTAMP as mart_created_at
                FROM scd_enhanced
            )

            SELECT * FROM final`;
        await fs.writeFile(`${this.dbtProjectPath}/models/marts/mart_${baseTableName}.sql`, martsSQL);
        console.log(`Generated: mart_${baseTableName}.sql`);
    }

    generateSCDFields(primaryKey) {
        return `        -- valid_from is the date this version of the record became active.
        CASE 
            WHEN table_type = 'dimension' THEN _publication_date
            ELSE NULL
        END as valid_from,
        
        -- valid_to is the date this version was superseded by a new one.
        -- It is calculated by "looking ahead" to the next record's publication date.
        CASE 
            WHEN table_type = 'dimension' THEN 
                LEAD(_publication_date) OVER (
                    PARTITION BY ${primaryKey}
                    ORDER BY _publication_date
                )
            ELSE NULL
        END as valid_to,
        
        -- is_current is a flag indicating the current version of a record.
        -- A record is current if it's approved and no newer version exists.
        CASE 
            WHEN review_status = 'R' THEN 'R'
            WHEN review_status = 'Y' AND table_type = 'fact' THEN 'Y'
            WHEN review_status = 'Y' AND table_type = 'dimension' AND 
                 LEAD(_publication_date) OVER (
                     PARTITION BY ${primaryKey}
                     ORDER BY _publication_date
                 ) IS NULL THEN 'Y'
            ELSE 'N'
        END as is_current,`;
    }

    generateChangeTypeLogic(primaryKey) {
        return `        -- Change type classification
        CASE 
            WHEN table_type = 'fact' THEN 'INSERT'
            WHEN table_type = 'dimension' AND LAG(_publication_date) OVER (
                PARTITION BY ${primaryKey}
                ORDER BY _publication_date
            ) IS NULL THEN 'INSERT'
            WHEN table_type = 'dimension' THEN 'UPDATE'
            ELSE 'INSERT'
        END as change_type,`;
    }

    async writeYamlFile(filePath, config) {
        const path = require('path');
        await fs.mkdir(path.dirname(filePath), { recursive: true });
        const yamlContent = yaml.dump(config, { 
            defaultFlowStyle: false,
            lineWidth: -1
        });
        await fs.writeFile(filePath, yamlContent);
    }
}

module.exports = { MetadataAwareGenerator };