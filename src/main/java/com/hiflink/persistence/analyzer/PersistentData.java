package com.hiflink.persistence.analyzer;

import com.hiflink.common.utils.Config.ConfigHelper;
import com.hiflink.common.utils.Schemas.ISchemaResgistry;
import com.hiflink.common.utils.Schemas.ISchemaSolver;
import com.hiflink.common.utils.resources.impl.ResourceManager;
import com.hiflink.persistence.exceptions.MalformedIANFileException;
import com.hiflink.persistence.exceptions.SchemaRecoveryException;
import com.typesafe.config.Config;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class PersistentData {
    private String configFile;
    private Log logger = LogFactory.getLog(PersistentData.class);
    private Config config;
    String[] indexFields;
    String[] indexKeys;
    String indexMap;
    String indexKey;
    String schemaSchemaRegistryName;
    String schemaFile;
    boolean schemaRegistryON = false;
    String schemaRegistryUrl;
    private boolean isPrincipal = false;
    protected String mapNamePrefix;
    //   final String schemaSufix = "";
    ISchemaSolver customSchemaNameStrategy;
    ISchemaResgistry iSchemaResgistry;
    String schemaSolverInstanceTypeName;
    String schemaRegistryInstanceTypeName;
    boolean isOutOfIndexationProcess = false;


    protected int lastVersionSchema;

    public PersistentData() {
    }

    public PersistentData(String configFile, Config appConfig) throws SchemaRecoveryException, IOException, MalformedIANFileException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        this.configFile = configFile;
        initConfig(appConfig);
    }

    private void initConfig(Config appConfig) throws MalformedIANFileException, SchemaRecoveryException, IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        logger.info("Loading configuration.");
        logger.info("Config path: " + configFile);
        logger.info("Loading configuration.");

        //config =  ConfigFactory.parseProperties(ResourceManager.getPropertiesFromResources(this.getClass(), configFile)) : appConfig;
        config = appConfig;
        customSchemaNameStrategy = initSchemasMapStrategy(appConfig);
        schemaSolverInstanceTypeName = new ConfigHelper().resolveConfigValue(appConfig, "schema.solver.instance", Arrays.asList(configFile, "schema", "solver", "instance"));
        schemaRegistryInstanceTypeName = new ConfigHelper().resolveConfigValue(appConfig, "schemaRegistry.instance", Arrays.asList( "schemaRegistry", "instance"));
        iSchemaResgistry = ISchemaResgistry.newSchemaRegistry(schemaRegistryInstanceTypeName);
        mapNamePrefix = appConfig.hasPath(configFile + ".index.schema.prefix") ? appConfig.getString(configFile + ".index.schema.prefix") : appConfig.getString("store.default.alias");

        String indexFieldsConfig = appConfig.getString(configFile + ".index.fields");
        if (indexFieldsConfig.isEmpty()) {
            throw new MalformedIANFileException("Define index fields in configuration file with: 'index.fields'");
        }
        indexFields = indexFieldsConfig.split(",");
        String indexKeysConfig = appConfig.getString(configFile + ".index.keys");
        if (indexKeysConfig.isEmpty()) {
            throw new MalformedIANFileException("Define key fields in configuration file with: 'index.keys'");
        }
        indexKeys = indexKeysConfig.split(",");

        schemaRegistryON = appConfig.getBoolean("schemaRegistry.schemaRegistryOn");
        if (schemaRegistryON) {
            schemaRegistryUrl = appConfig.getString("schemaRegistry.url");
            //gets from schema registry
            schemaSchemaRegistryName = appConfig.getString(configFile + ".index.schema.name");
            if (schemaSchemaRegistryName.isEmpty()) {
                throw new MalformedIANFileException("Define schema name at Schema Registry in configuration file with: 'index.schema.name'");
            }
            schemaFile = appConfig.hasPath(configFile + ".index.schema.path") ? appConfig.getString(configFile + ".index.schema.path") : null;

        } else {
            //gets form index directory
            schemaFile = appConfig.getString(configFile + ".index.schema.path");
            if (schemaFile.isEmpty()) {
                throw new MalformedIANFileException("Define schema file path in configuration file with: 'index.schema.filePath'");
            }
        }
        isPrincipal = appConfig.hasPath(configFile + ".index.principal") ? appConfig.getBoolean(configFile + ".index.principal") : false;
        isOutOfIndexationProcess = appConfig.hasPath(configFile + ".shaded.index") ? appConfig.getBoolean(configFile + ".shaded.index") : false;
        //Create key
        createKey();
        //Create persistent map name
        this.indexMap = this.getSchema().getName();

    }


    protected ISchemaSolver initSchemasMapStrategy(Config strategyConfig) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        ISchemaSolver schemaNameStrategy = ISchemaSolver.newSchemaSolver(schemaSolverInstanceTypeName, strategyConfig, this.configFile);
        return schemaNameStrategy;
    }

    protected ISchemaSolver initSchemasMapStrategy(Config strategyConfig, String conceptName) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        ISchemaSolver schemaNameStrategy = ISchemaSolver.newSchemaSolver(schemaSolverInstanceTypeName, strategyConfig, conceptName);
        return schemaNameStrategy;
    }

    private void createKey() {
        if (indexKeys.length == 1) {
            indexKey = indexKeys[0].replace("\"", "");
        } else {
            List<String> keyList = new ArrayList<String>();
            for (String key : indexKeys) {
                keyList.add(key);
            }

            java.util.Collections.sort(keyList);

            indexKey = "";
            for (String s : keyList) {
                if ("".equals(indexKey)) {
                    indexKey = s;
                } else {
                    indexKey = indexKey + "_" + s;
                }
            }
        }
    }


    public HashMap<String, Object> analyze(GenericRecord genericRecord) {
        HashMap<String, Object> indexableValues = new HashMap<>();
        for (String indexField : indexFields) {
            indexableValues.put(indexField, genericRecord.get(indexField));
        }
        return indexableValues;
    }


    public void analyzeQuery(String query) throws Exception {
        throw new Exception("Not implemented yet");
    }

    public String[] getIndexKeys() {
        return indexKeys;
    }

    public String[] getIndexFields() {
        return indexFields;
    }

    public String getIndexMap() {
        return indexMap;
    }

//    public String getSchemaIndexMap() {
//        return this.getIndexMap() + schemaSufix;
//    }

    public String getKey() {
        return indexKey;
    }

    public boolean getIsPrincipal() {
        return isPrincipal;
    }

    public String getSchemaFilePath() {
        return schemaFile;
    }

    public Schema getSchema() throws SchemaRecoveryException, IOException {
        Schema loadedSchema = null;

        if (schemaRegistryON) {
            //gets from schema registry
            //TODO: Implements recover form schema registry
            logger.debug("Se busca el esquema para persistent data: " + customSchemaNameStrategy.getSubjectName());
            String schemaToString = iSchemaResgistry.getLatest_Schema(schemaRegistryUrl, customSchemaNameStrategy);
            try {
                lastVersionSchema = iSchemaResgistry.getLastVersion(schemaRegistryUrl, customSchemaNameStrategy);
            } catch (RuntimeException e) {
                e.printStackTrace();
            }
            if (schemaToString == null && schemaFile != null) {
//                String temporalChangeRecoverType = ResourceManager.resourcesMode;
//                ResourceManager.resourcesMode = "file";
                try {
                    String loadedFile = ResourceManager.getContentFromResources(this.getClass(), schemaFile, "file", "");
//                //gets form index directory
                    if (loadedFile == null || (loadedFile != null && loadedFile.isEmpty())) {
                        throw new SchemaRecoveryException("Schema for: " + schemaFile + " is not present. Check your Schema Registry connection.");
                    }
                    loadedSchema = new Schema.Parser().parse(loadedFile);
                } finally {
//                    ResourceManager.resourcesMode = temporalChangeRecoverType;
                }


//                String loadedFile = ResourceManager.getContentFromResources(this.getClass(), schemaFile);
//                //gets form index directory
//                loadedSchema = new Schema.Parser().parse(loadedFile);

            } else {
                loadedSchema = new Schema.Parser().parse(schemaToString);
            }
        } else {
            //File loadedFile = new File(schemaFile);
            String loadedFile = ResourceManager.getContentFromResources(this.getClass(), schemaFile, "file", "");
            //gets form index directory
            loadedSchema = new Schema.Parser().parse(loadedFile);
        }
        return loadedSchema;
    }


    public int getLastVersionSchema() {
        return lastVersionSchema;
    }

    protected String getHazelcastSchemaName() {
        return this.mapNamePrefix + this.schemaSchemaRegistryName;
    }
}

