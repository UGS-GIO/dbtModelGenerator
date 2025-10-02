// dbt-model-generator/index.js
// Cloud Function that generates dbt models after data ingestion
// Triggered by Pub/Sub messages from gdb-ingest-processor

const { Storage } = require('@google-cloud/storage');
const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');
const { MetadataAwareGenerator } = require('./metadata-aware-generator');

const storage = new Storage();
const secretManager = new SecretManagerServiceClient();

const PROJECT_ID = process.env.GCP_PROJECT || 'ut-dnr-ugs-backend-tools';
const DBT_BUCKET = 'ugs_dbt_models';

/**
 * Load database credentials from Secret Manager
 */
async function loadDatabaseCredentials() {
  try {
    console.log('Loading database credentials from Secret Manager...');
    
    const [userResponse] = await secretManager.accessSecretVersion({
      name: `projects/${PROJECT_ID}/secrets/SEAMLESSGEOLMAP_INGEST_USER/versions/latest`
    });
    
    const [passwordResponse] = await secretManager.accessSecretVersion({
      name: `projects/${PROJECT_ID}/secrets/SEAMLESSGEOLMAP_INGEST_PASS/versions/latest`
    });
    
    return {
      instance_connection_name: 'ut-dnr-ugs-mappingdb-prod:us-west3:mapping-db',
      database: 'seamlessgeolmap',
      user: userResponse.payload.data.toString(),
      password: passwordResponse.payload.data.toString()
    };
  } catch (error) {
    console.error('Failed to load database credentials from Secret Manager:', error);
    throw error;
  }
}

/**
 * Upload generated dbt files to GCS bucket
 */
async function uploadFilesToGCS(localPath, basePath) {
  const fs = require('fs').promises;
  const path = require('path');
  
  const files = await fs.readdir(localPath, { recursive: true });
  const uploads = [];
  
  for (const file of files) {
    const fullPath = path.join(localPath, file);
    const stat = await fs.stat(fullPath);
    
    if (stat.isFile()) {
      const gcsPath = `${basePath}/${file}`;
      console.log(`Uploading ${fullPath} to gs://${DBT_BUCKET}/${gcsPath}`);
      
      uploads.push(
        storage.bucket(DBT_BUCKET).upload(fullPath, {
          destination: gcsPath,
          metadata: {
            contentType: file.endsWith('.sql') ? 'text/sql' : 
                        file.endsWith('.yml') ? 'text/yaml' : 'text/plain',
            metadata: {
              generatedAt: new Date().toISOString(),
              generator: 'dbt-model-generator-function'
            }
          }
        })
      );
    }
  }
  
  await Promise.all(uploads);
  console.log(`Uploaded ${uploads.length} files to GCS`);
}

/**
 * Check if models need regeneration
 * Only regenerate if:
 * 1. This is a new base table (no existing models)
 * 2. Schema version changed
 */
async function shouldRegenerateModels(baseTableName, schemaVersion) {
  try {
    // Check if staging model already exists
    const [files] = await storage.bucket(DBT_BUCKET).getFiles({
      prefix: `models/staging/stg_${baseTableName}.sql`
    });
    
    if (files.length === 0) {
      console.log(`No existing models found for ${baseTableName} - will generate`);
      return true;
    }
    
    // Check schema version in metadata
    // If version changed (v1 -> v2), we need to regenerate
    const [metadata] = await storage.bucket(DBT_BUCKET).getFiles({
      prefix: `models/staging/.${baseTableName}_schema_version`
    });
    
    if (metadata.length === 0) {
      console.log(`No schema version tracked - will regenerate`);
      return true;
    }
    
    const [versionFile] = metadata;
    const [content] = await versionFile.download();
    const existingVersion = content.toString('utf8').trim();
    
    if (existingVersion !== schemaVersion) {
      console.log(`Schema version changed: ${existingVersion} -> ${schemaVersion} - will regenerate`);
      return true;
    }
    
    console.log(`Models already exist for ${baseTableName} ${schemaVersion} - skipping`);
    return false;
    
  } catch (error) {
    console.error('Error checking if regeneration needed:', error);
    return true; // Default to regenerating on error
  }
}

/**
 * Track the schema version for this base table
 */
async function trackSchemaVersion(baseTableName, schemaVersion) {
  const versionFile = `.${baseTableName}_schema_version`;
  await storage.bucket(DBT_BUCKET).file(`models/staging/${versionFile}`).save(schemaVersion);
  console.log(`Tracked schema version ${schemaVersion} for ${baseTableName}`);
}

/**
 * Main Pub/Sub handler
 */
exports.generateDbtModels = async (message, context) => {
  const startTime = Date.now();
  console.log('='.repeat(80));
  console.log('DBT Model Generator triggered');
  console.log('Event ID:', context.eventId);
  console.log('Timestamp:', context.timestamp);
  
  let dbClient = null;
  
  try {
    // Decode Pub/Sub message
    const messageData = message.data 
      ? Buffer.from(message.data, 'base64').toString() 
      : '{}';
    const payload = JSON.parse(messageData);
    
    console.log('Received payload:', payload);
    
    const { 
      baseTableName, 
      schemaVersion = 'v1',
      operation = 'new_table'  // 'new_table' or 'schema_change'
    } = payload;
    
    if (!baseTableName) {
      throw new Error('baseTableName is required in Pub/Sub message');
    }
    
    console.log(`Processing: ${baseTableName} (${schemaVersion}, ${operation})`);
    
    // Check if regeneration is needed
    const shouldRegenerate = await shouldRegenerateModels(baseTableName, schemaVersion);
    
    if (!shouldRegenerate) {
      console.log('Skipping regeneration - models are up to date');
      return;
    }
    
    // Load database credentials
    console.log('Loading database credentials...');
    const dbConfig = await loadDatabaseCredentials();
    
    // Use Cloud SQL Unix socket in Cloud Run/Functions
    const dbConnection = {
      host: `/cloudsql/${dbConfig.instance_connection_name}`,
      database: dbConfig.database,
      user: dbConfig.user,
      password: dbConfig.password,
      port: 5432
    };
    
    console.log('Connecting to database...');
    const { Client } = require('pg');
    dbClient = new Client(dbConnection);
    await dbClient.connect();
    console.log('Database connected successfully');
    
    // Create temporary directory for dbt project
    const os = require('os');
    const path = require('path');
    const fs = require('fs').promises;
    
    const tempDir = path.join(os.tmpdir(), `dbt-${Date.now()}`);
    await fs.mkdir(tempDir, { recursive: true });
    await fs.mkdir(path.join(tempDir, 'models'), { recursive: true });
    await fs.mkdir(path.join(tempDir, 'models/staging'), { recursive: true });
    await fs.mkdir(path.join(tempDir, 'models/intermediate'), { recursive: true });
    await fs.mkdir(path.join(tempDir, 'models/marts'), { recursive: true });
    
    console.log(`Created temp directory: ${tempDir}`);
    
    // Initialize generator
    const generator = new MetadataAwareGenerator(dbConnection, tempDir);
    generator.db = dbClient; // Use our connected client
    
    // Generate all models
    console.log(`Generating dbt models for ${baseTableName}...`);
    await generator.generateAllModelsForFirstLoad(baseTableName);
    
    // Upload generated files to GCS
    console.log('Uploading generated models to GCS...');
    await uploadFilesToGCS(
      path.join(tempDir, 'models'),
      `models/${baseTableName}_${schemaVersion}`
    );
    
    // Track schema version
    await trackSchemaVersion(baseTableName, schemaVersion);
    
    // Cleanup temp directory
    await fs.rm(tempDir, { recursive: true, force: true });
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    console.log(`\n${'='.repeat(80)}`);
    console.log(`✅ Successfully generated dbt models for ${baseTableName}`);
    console.log(`   Location: gs://${DBT_BUCKET}/models/${baseTableName}_${schemaVersion}/`);
    console.log(`   Duration: ${duration}s`);
    console.log('='.repeat(80));
    
  } catch (error) {
    console.error('❌ Error generating dbt models:', error);
    console.error('Stack trace:', error.stack);
    throw error; // Let Pub/Sub retry
    
  } finally {
    if (dbClient) {
      await dbClient.end();
      console.log('Database connection closed');
    }
  }
};