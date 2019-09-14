package com.hiflink.persistence.index;

import com.hiflink.common.utils.Config.ConfigHelper;
import com.hiflink.persistence.analyzer.IndexDataCollection;
import com.hiflink.persistence.analyzer.IndexDirection;
import com.hiflink.persistence.exceptions.MalformedIANFileException;
import com.hiflink.persistence.exceptions.SchemaRecoveryException;
import com.hiflink.persistence.translators.ITranslatorManager;
import com.typesafe.config.Config;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class IndexManager {

    private IndexDataCollection indexCollectionDN;
    private String configFilePath;
    private Config config;
    private Log logger = LogFactory.getLog(IndexManager.class);
    private ITranslatorManager indexTranslatorManager;
    private Pattern pattern;
    private String mode;
    private String cloudPath;
    private String translatorInstancename;
    @Getter
    private String storageInstanceName;
    @Getter
    private String searcherInstanceName;


    public IndexManager(String configFilePath, IndexDirection indexDirection, boolean continueIndexSchemaNotFound, String mode, String cloudPath) throws MalformedIANFileException, IOException, SchemaRecoveryException, IllegalAccessException, ClassNotFoundException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        this(configFilePath, indexDirection, false, continueIndexSchemaNotFound, mode, cloudPath);

    }

    public IndexManager(String configFilePath, IndexDirection indexDirection, Boolean forceReload, Boolean continueIndexSchemaNotFound, String mode, String cloudPath) throws MalformedIANFileException, IOException, SchemaRecoveryException, IllegalAccessException, InstantiationException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException {
        pattern = Pattern.compile(".*\\/[A-z]*\\.ian$");
        this.configFilePath = configFilePath;
        indexCollectionDN = new IndexDataCollection();
        this.mode = mode;
        this.cloudPath = cloudPath;

        loadConfig(this.mode,this.cloudPath);

        initTranslator();
        initAnalyzers(indexDirection, forceReload, continueIndexSchemaNotFound);


    }

    private void loadConfig(String inMode, String inCloudPath) throws IOException {
        logger.info("Loading configuration.");
        logger.info("Config path: " + configFilePath);
        logger.info("Loading configuration.");
        config = new ConfigHelper().loadConfig(this.getClass(),configFilePath,inMode,inCloudPath);
        translatorInstancename = new ConfigHelper().resolveConfigValue(config,"index.translator.instance.name",Arrays.asList("index","translator","instance","name"));
        storageInstanceName = new ConfigHelper().resolveConfigValue(config,"index.storage.instance.name",Arrays.asList("index","storage","instance","name"));
        searcherInstanceName = new ConfigHelper().resolveConfigValue(config,"index.searcher.instance.name",Arrays.asList("index","searcher","instance","name"));
    }

    private void initTranslator() throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        indexTranslatorManager = ITranslatorManager.newTranslatorManager(translatorInstancename);
    }

    private void initAnalyzers(IndexDirection indexDirection, boolean forceReload, boolean continueIndexSchemaNotFound) throws MalformedIANFileException, IOException, SchemaRecoveryException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        final Collection<String> list = this.config.hasPath("index.ian.files") ? Arrays.stream(this.config.getString("index.ian.files").split(",")).collect(Collectors.toSet()) : null;

        if (list != null && list.size() > 0) {
            Collection<String> analyzedList = list.stream().map(z -> new File(z).getName()).distinct().collect(Collectors.toList());
            logger.info("Analizer for: " + analyzedList.size() + " files.");
            for (String indexFile : analyzedList) {
                logger.info("Analizer for: " + indexFile);
                loadPersistentData(indexFile, config);
            }
        }
        recoverSchema(indexDirection, config, forceReload, continueIndexSchemaNotFound);

    }

    private void initAnalyzers(IndexDirection indexDirection, boolean continueIndexSchemaNotFound) throws Exception {
        this.initAnalyzers(indexDirection, false, continueIndexSchemaNotFound);

    }

    private void loadPersistentData(String indexFile, Config appConfig) throws IOException, SchemaRecoveryException, MalformedIANFileException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        indexCollectionDN.addPersistentData(indexFile, appConfig);
    }


    public Config getConfig() {
        return config;
    }

    //TODO:Creates index schema
    public void recoverSchema(IndexDirection indexDirection, Config appConfig, Boolean forceReloaded, Boolean continueIndexSchemaNotFound) throws SchemaRecoveryException, IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        indexCollectionDN.recoverSchema(indexDirection, appConfig, forceReloaded, continueIndexSchemaNotFound);

    }

    private void createSchema() throws IOException, SchemaRecoveryException {
        indexCollectionDN.createSchemas();

    }


    public Schema getIndexationSchema(String mapName) {
        return this.indexCollectionDN.getIndexationSchema(mapName);
    }

    public Schema getSchema(String typeName) throws IOException, SchemaRecoveryException {
        return this.indexCollectionDN.getSchema(typeName);
    }


    public int getLastVersion(String typeName) {
        return this.indexCollectionDN.getLastVersion(typeName);
    }

    public int getLastVersionIndex(String key){
        return this.indexCollectionDN.getLastVersionIndex(key);
    }


    public Map<String, String> getMapTypes(String mapName) throws SchemaRecoveryException {

        return indexCollectionDN.getMapTypes(mapName);
    }

    public String[] getFields(String mapName) {
        return this.indexCollectionDN.getFields(mapName);
    }

    public String getKey(String mapName, GenericRecord genericRecord) {
        String indexKey = "";
        String[] keys = this.indexCollectionDN.getKeys(mapName);
        if (keys.length == 1) {
            indexKey = genericRecord.get(keys[0]).toString();
        } else {
            List<String> keyList = new ArrayList<String>();
            for (String key : keys) {
                keyList.add(key);
            }

            Collections.sort(keyList);

            indexKey = "";
            for (String s : keyList) {
                if ("".equals(indexKey)) {
                    indexKey = genericRecord.get(s).toString();
                } else {
                    indexKey = indexKey + "_" + genericRecord.get(s).toString();
                }
            }
        }
        return indexKey;

    }




    public List<String> getIndexMaps() {
        return this.indexCollectionDN.getMaps();
    }

    public Collection<? extends String> getIndexMaps(String indexName) {
        return this.indexCollectionDN.getIndexMaps(indexName);
    }

    public GenericRecord getGenericRecord(String mapName, GenericRecord genericRecord, Optional<GenericRecord> oldRecord) {
        boolean isNewSchema = false;
        //When a new generic record arrives.
        // from index, search this data. if exist update index else create new record
        Schema indexSchema = this.indexCollectionDN.getIndexationSchema(mapName);

        String[] indexFields = this.indexCollectionDN.getFields(mapName);

        GenericRecord indexRecord = oldRecord.orElse(new GenericData.Record(indexSchema));
        if (!indexRecord.getSchema().equals(indexSchema)) {
            indexRecord = new GenericData.Record(indexSchema);
            isNewSchema = oldRecord != null && oldRecord.isPresent();
        }

        for (String indexField : indexFields) {
            //IndexField está en formato traducido y hay que ponerlo en formato sin traducir
            String translateField = indexTranslatorManager.translateField(indexField);
            Object recordValue = genericRecord.get(translateField);
            if (isNewSchema && (recordValue == null || recordValue.toString().isEmpty())) {
                recordValue = oldRecord.get().get(translateField);
            }
            Object translatevalue = indexTranslatorManager.translateValue(indexField, recordValue);

            // indexRecord.put(mapName + "_" + indexField, genericRecord.get(indexField));
            indexRecord.put(translateField, translatevalue);
        }


        return indexRecord;


    }

    public String getIndexMapName(String mapName) {
        return this.indexCollectionDN.getIndexMapName(mapName);
    }

    public String getPersistentMapName(String mapName) {
        return this.indexCollectionDN.getPersistentMapName(mapName);
    }

    public String getPaginationMap(String mapName) {
        return this.indexCollectionDN.getPaginationMap(mapName);
    }

    public String getPrincipal(String mapName) {
        return this.indexCollectionDN.getPrincipal(mapName);
    }

    public Schema getPaginationSchema(String mapName) {
        return this.indexCollectionDN.getPaginationSchema(mapName);
    }

    public boolean isIndexable(String mapName) {
        return this.indexCollectionDN.isIndexable(mapName);
    }

    public String shadedMapName(String mapName) {
        return this.indexCollectionDN.shadedMapName(mapName);
    }

    public Map<String, Object> translate(Map<String, Object> mapSearch) {
        Map<String, Object> tmp_translated = indexTranslatorManager.translateToIndex(mapSearch);
        Map<String, Object> mapSearchTranslated = new HashMap<>();


        for (Map.Entry<String, Object> entry : tmp_translated.entrySet()) {
            if (entry.getValue() instanceof Map) {
                mapSearchTranslated.put(entry.getKey(), this.translate((Map<String, Object>) entry.getValue()));
            } else {
                Object value = entry.getValue();
                Object translateValues;
                if(value instanceof List){
                    List listValues = (List) value;
                    translateValues = translateStringValues(listValues);
                }else {
                    translateValues = (value instanceof String) ? String.valueOf(value).toUpperCase() : value;
                }
                mapSearchTranslated.put(entry.getKey(), translateValues);
            }
        }

        return mapSearchTranslated;
    }


    public List<String> getIndexMapsByKey(String key) {
        return indexCollectionDN.getIndexMapsByKey(key);
    }


    public String getIndexMapNameByKey(String key) {
        return indexCollectionDN.getIndexMapNameByKey(key);
    }

    public Schema getIndexationSchemaByKey(String key) {
        return indexCollectionDN.getIndexationSchemaByKey(key);
    }

    public String getPaginationMapByKey(String key) {
        return this.indexCollectionDN.getPaginationMapByKey(key);

    }

    public Schema getPaginationSchemaByKey(String indexKey) {
        return this.indexCollectionDN.getPaginationSchemaByKey(indexKey);
    }

    public List<String> getIndexKeys(){

        return this.indexCollectionDN.getIndexKeys();

    }

    public String getIndexKey(String mapName) {
        return this.indexCollectionDN.getIndexKey(mapName);
    }

    private List translateStringValues(List values){
        List<Object> result =  new ArrayList<>();
        for (Object obj: values) {
            if(obj instanceof String) result.add(String.valueOf(obj).toUpperCase()); else result.add(obj);
        }
        return result;
    }


}
