package com.hiflink.persistence.analyzer;

import com.hiflink.common.utils.Config.ConfigHelper;
import com.hiflink.common.utils.Schemas.ISchemaResgistry;
import com.hiflink.common.utils.Schemas.ISchemaSolver;
import com.hiflink.common.utils.avro.AvroUtils;
import com.hiflink.persistence.exceptions.IndexSchemaRecoveryException;
import com.hiflink.persistence.exceptions.SchemaRecoveryException;
import com.typesafe.config.Config;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class IndexData extends PersistentData {
    private HashMap<String, PersistentData> persistentDataCollection;
    private String indexName;
    private Schema schema;
    private Schema paginationSchema;
    private SchemaBuilder.FieldAssembler<Schema> assembler;
    private HashMap<String, String> mapTypes;
    private static Log logger;
    private String paginationMap;
    private final String paginationPREName = "CURSOR_";
    private final String unknownCursor = "CURSOR_";
    //private final String unknownCursor = "CURSOR_";
    private final String indexPREName = "idx_";
    private String schemaPaginationSchemaRegistryName = "";
    private String principalName;
    private String topicName;
    private String cursorPrefix;
    private String indexPrefix;

    ISchemaSolver index_customSchemaNameStrategy;
    ISchemaSolver cursor_customSchemaNameStrategy;
    String schemaRegistryInstanceTypeName;
    ISchemaResgistry iSchemaResgistry;


    //--------------------- Constructor region ------------------------//
    public IndexData(String key) {
        logger = LogFactory.getLog(IndexData.class);
        this.indexKey = key;
        persistentDataCollection = new HashMap<>();
        //TODO: Start and load mapTypes
        mapTypes = new HashMap<>();
    }
    //--------------------- EOCR ------------------------//

    //--------------------- etters region ------------------------//
    public String getKey() {
        return indexKey;
    }

    public Schema getSchema() {
        return schema;
    }

    public Schema getPaginationSchema() {
        return this.paginationSchema;
    }

    public Schema getSchema(String typeName) throws SchemaRecoveryException, IOException {
        PersistentData persistentData = this.persistentDataCollection.get(typeName.toLowerCase());
        return persistentData.getSchema();
    }

    public String[] getFields(String mapName) {
        return this.persistentDataCollection.get(mapName.toLowerCase()).getIndexFields();
    }

    public Map<String, String> getMapTypes() throws SchemaRecoveryException {
        Map<String, String> mapTypes = new HashMap<String, String>();
        Schema schemaFromRegistry = this.getSchemaFromRegistry(this.indexName, index_customSchemaNameStrategy);
        if (schemaFromRegistry == null) {
            schemaFromRegistry = this.schema;
        }
        if (schemaFromRegistry != null) {
            mapTypes = new AvroUtils().getParameters(schemaFromRegistry.toString());
        }
        return mapTypes;
    }

    //--------------------- EOER ------------------------//


    //--------------------- Methods region ------------------------//
    public void addPersistentData(PersistentData persistentData) {
        if (!this.persistentDataCollection.containsKey(persistentData.getIndexMap().toLowerCase())) {
            this.persistentDataCollection.put(persistentData.getIndexMap().toLowerCase(), persistentData);
            principalName = principalName == null && persistentData.getIsPrincipal() ? persistentData.getIndexMap() : null;
        }
    }

    public boolean containsMap(String mapName) {
        return this.persistentDataCollection.containsKey(mapName.toLowerCase());
    }

    public boolean isMap(String mapName) {
        return this.indexName.toLowerCase().equals(mapName.toLowerCase());
    }

    public boolean hasTypesFor(String mapName) {
        return this.persistentDataCollection.containsKey(mapName.toLowerCase()) || this.getIndexMapName().equals(mapName);
    }

    public void recoverSchema(IndexDirection indexDirection, Config appconfig, Boolean forceReload) throws SchemaRecoveryException, IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        String schemaName = getSchemaName(this.persistentDataCollection);
        String paginationSchemaName = unknownCursor + schemaName;

        index_customSchemaNameStrategy = initSchemasMapStrategy(appconfig, schemaName);
        cursor_customSchemaNameStrategy = initSchemasMapStrategy(appconfig, paginationSchemaName);
        schemaRegistryInstanceTypeName = new ConfigHelper().resolveConfigValue(appconfig,"schemaRegistry.instance", Arrays.asList("schemaRegistry", "instance"));
        iSchemaResgistry = ISchemaResgistry.newSchemaRegistry(schemaRegistryInstanceTypeName);
        super.schemaRegistryON = appconfig.getBoolean("schemaRegistry.schemaRegistryOn");
        super.schemaRegistryUrl = appconfig.getString("schemaRegistry.url");
        this.indexPrefix = appconfig.hasPath(schemaSchemaRegistryName + ".index.schema.prefix") ? appconfig.getString(schemaSchemaRegistryName + ".index.schema.prefix") : appconfig.getString("hazelcast.default.alias");
        this.cursorPrefix = appconfig.hasPath(schemaPaginationSchemaRegistryName + ".index.schema.prefix") ? appconfig.getString(schemaPaginationSchemaRegistryName + ".index.schema.prefix") : appconfig.getString("hazelcast.default.alias");
        logger.info("Se recuperan los esquemas: " + schemaSchemaRegistryName + " y " + schemaPaginationSchemaRegistryName);
        //Search for schema in Schema Registry
        try {
            this.schema = getSchemaFromRegistry(schemaSchemaRegistryName, index_customSchemaNameStrategy);
        } catch (Exception e) {
            logger.info("Not founded schema for: " + schemaSchemaRegistryName + ". It'll be created.");
        }
        try {
            this.paginationSchema = getSchemaFromRegistry(schemaPaginationSchemaRegistryName, cursor_customSchemaNameStrategy);
        } catch (Exception e) {
            logger.info("Not founded schema for: " + schemaPaginationSchemaRegistryName + ". It'll be created.");
        }
        topicName = appconfig.hasPath("schemaRegistry.name.strategy.topic") ? appconfig.getString("schemaRegistry.name.strategy.topic") : "com.santander.mic.schemas";
        if (this.schema == null || forceReload) {
            if (indexDirection == IndexDirection.consumer) {
                if (forceReload && this.schema != null) {
                    this.paginationSchema = createPaginationSchema(schema);
                } else {
                    throw new IndexSchemaRecoveryException("Searched schema do not exists. " + schemaName);
                }
            } else {
                createSchema();
                this.paginationSchema = createPaginationSchema(schema);
            }
        } else {
            if (this.paginationSchema == null) {
                this.paginationSchema = createPaginationSchema(schema);
            }
        }

    }

    private Schema getSchemaFromRegistry(String schemaName, ISchemaSolver schemaNameStrategy) throws SchemaRecoveryException {
        Schema loadedSchema = null;

        if (schemaRegistryON) {
            //gets from schema registry
            //TODO: Implements recover form schema registry
            try {
                logger.debug("Se busca el esquema para index data: " + schemaNameStrategy.getSubjectName());
                String schemaToString = iSchemaResgistry.getLatest_Schema(schemaRegistryUrl, schemaNameStrategy);
                lastVersionSchema = iSchemaResgistry.getLastVersion(schemaRegistryUrl, schemaNameStrategy);

                if (schemaToString != null) {
                    loadedSchema = new Schema.Parser().parse(schemaToString);
                } else {
                    logger.info("Fallo al reguperar el esquema: " + schemaName + " este error se gestiona junto con los índices.");
                    throw new SchemaRecoveryException("Schema for: " + schemaName + " is not present. Check your Schema Registry connection.");
                }
            } catch (Exception e) {
                logger.error("Fallo al reguperar el esquema: " + schemaName + " este error se gestiona junto con los índices.", e);
                throw new SchemaRecoveryException("Schema for: " + schemaName + " is not present. Check your Schema Registry connection.");
                // return null;
            }
        }
        return loadedSchema;
    }

    private Schema createPaginationSchema(Schema indexSchema) {
        //String paginationSchemaName = unknownCursor + indexSchema.getName();
        String paginationSchemaName = this.schemaPaginationSchemaRegistryName;
        SchemaBuilder.FieldAssembler<Schema> assembler = SchemaBuilder
                .record(paginationSchemaName)
                .namespace(indexSchema.getNamespace())
                .fields();
        assembler
                //.name(analyzerEntry.getValue().getIndexMap() + "_" + currentField.name())
                .name("cursorItems")
                .type()
                .array()
                .items()
                .type(indexSchema)
                .noDefault();
        Schema currentSchema = assembler.endRecord();

        return currentSchema;

    }

    public void createSchema() throws IOException, SchemaRecoveryException {
        String schemaName = getSchemaName(this.persistentDataCollection);
        List<String> appendedNames = new ArrayList<>();

        SchemaBuilder.FieldAssembler<Schema> assembler = SchemaBuilder
                .record(schemaName)
                .namespace(topicName)
                .fields();
//TODO: A la hora de crear el esquema hay que tener en cuenta que ya tenemos que ser capaces de traducir, porque el nombre en el esquema tiene que ser el nombre traducido
        //Gets analyzer schema and creates a new schema using indexed fields
        for (Map.Entry<String, PersistentData> analyzerEntry : this.persistentDataCollection.entrySet()) {
            Schema loadedSchema = analyzerEntry.getValue().getSchema();

            String[] currentFields = analyzerEntry.getValue().getIndexFields();
            for (String currentFieldName : currentFields) {
                Schema.Field currentField = loadedSchema.getField(currentFieldName);
                if (!appendedNames.contains(currentField.name())) {
                    assembler
                            //.name(analyzerEntry.getValue().getIndexMap() + "_" + currentField.name())
                            .name(currentField.name())
                            .type(currentField.schema())
                            .noDefault();
                    appendedNames.add(currentField.name());
                }
            }
        }
        this.schema = assembler.endRecord();
    }


    private String getSchemaName(HashMap<String, PersistentData> analyzerCollection) {
        String schemaName = indexPREName + this.getKey();

        /*
        List<String> nameList = new ArrayList<String>();
        for (Map.Entry<String, PersistentData> analyzerEntry : this.persistentDataCollection.entrySet()) {
            nameList.add(analyzerEntry.getValue().getIndexMap());
        }

        for (String s : nameList) {
            schemaName = schemaName + "_" + s;
        }*/
        indexName = schemaName;
        paginationMap = paginationPREName + this.getKey();
        this.schemaSchemaRegistryName = indexName;
        this.schemaPaginationSchemaRegistryName = paginationMap;
        return schemaName;
    }

    public Collection<? extends String> getMaps() {
        List<String> indexMaps = new ArrayList<>();
        for (Map.Entry<String, PersistentData> entry : this.persistentDataCollection.entrySet()) {
            indexMaps.add(entry.getValue().getIndexMap());
        }
        indexMaps.add(indexName);
        indexMaps.add(paginationMap);
        return indexMaps;
    }

    public String getIndexMapName() {
        return indexName;
    }

    public String getPaginationMap() {
        return paginationMap;
    }

    public String getPrincipal() {
        return principalName;
    }

    public String[] getKeys() {
        List<String> searchedKeys = new ArrayList<>();
        for (Map.Entry<String, PersistentData> entry : this.persistentDataCollection.entrySet()) {
            for (String indexKey : entry.getValue().getIndexKeys()) {
                if (!searchedKeys.contains(indexKey)) {
                    searchedKeys.add(indexKey);
                }
            }

        }
        String[] recoveredKeys = new String[searchedKeys.size()];
        searchedKeys.toArray(recoveredKeys);
        return recoveredKeys;
    }

    public boolean containsKey(String key) {
        return this.getKey().equals(key);
    }

    //--------------------- EOMR ------------------------//


    public int getLastVersionSchema(String typeName) {
        if (this.indexName.equals(typeName)) {
            return lastVersionSchema;
        } else {
            PersistentData persistentData = this.persistentDataCollection.get(typeName.toLowerCase());
            return persistentData.getLastVersionSchema();
        }
    }

    public String getPersistentMapName(String mapName) {
        if (this.persistentDataCollection.containsKey(mapName.toLowerCase())) {
            return this.persistentDataCollection.get(mapName.toLowerCase()).getHazelcastSchemaName();
        } else if (this.indexName.equals(mapName)) {
            return this.indexPrefix + mapName;
        } else if (this.paginationMap.equals(mapName)) {
            return this.cursorPrefix + mapName;
        }

        return mapName;
    }

    public String getOutofIndexName() {
        if (this.persistentDataCollection.size() != 1) {
            return null;
        }
        Iterator<Map.Entry<String,PersistentData>> primero =  persistentDataCollection.entrySet().iterator();
        Map.Entry<String,PersistentData> evaluable = primero.next();
        return evaluable.getValue().isOutOfIndexationProcess ? evaluable.getValue().indexMap:null;
    }
}