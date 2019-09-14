package com.hiflink.persistence.analyzer;

import com.hiflink.persistence.exceptions.IndexSchemaRecoveryException;
import com.hiflink.persistence.exceptions.MalformedIANFileException;
import com.hiflink.persistence.exceptions.SchemaRecoveryException;
import com.typesafe.config.Config;
import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class IndexDataCollection {

    private Log logger = LogFactory.getLog(IndexDataCollection.class);

    private List<IndexData> indexList;

    public IndexDataCollection() {
        indexList = new ArrayList<>();
    }

    public void addPersistentData(String indexFile, Config appConfig) throws IOException, SchemaRecoveryException, MalformedIANFileException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        PersistentData persistentData = new PersistentData(indexFile, appConfig);
        this.getIndex(persistentData.getKey()).addPersistentData(persistentData);
    }

    public void addPersistentData(File indexFile, Config appConfig) throws IOException, SchemaRecoveryException, MalformedIANFileException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        PersistentData persistentData = new PersistentData(indexFile.getAbsolutePath(), appConfig);
        this.getIndex(persistentData.getKey()).addPersistentData(persistentData);
    }

    private IndexData getIndex(String indexKey) {
        IndexData searchedIndex = null;
        for (IndexData indexDN : indexList) {
            if (indexDN.getKey().equals(indexKey)) {
                searchedIndex = indexDN;
                break;
            }
        }
        if (searchedIndex == null) {
            searchedIndex = new IndexData(indexKey);
            this.indexList.add(searchedIndex);
        }
        return searchedIndex;
    }

    public void createSchemas() throws IOException, SchemaRecoveryException {
        //For echa index creates an schema
        for (IndexData indexDN : indexList) {
            indexDN.createSchema();
        }
    }

    public Schema getIndexationSchema(String mapName) {
        for (IndexData indexDN : this.indexList) {
            if (indexDN.containsMap(mapName)) {
                return indexDN.getSchema();
            }
        }
        return null;
    }


    public Schema getIndexationSchemaByKey(String key) {
        for (IndexData indexDN : this.indexList) {

            if (indexDN.containsKey(key)) {
                return indexDN.getSchema();
            }
        }
        return null;
    }


    public Schema getSchema(String typeName) throws SchemaRecoveryException, IOException {
        for (IndexData indexDN : this.indexList) {
            if (indexDN.containsMap(typeName)) {
                return indexDN.getSchema(typeName);
            }
        }
        return null;
    }


    public int getLastVersion(String typeName) {
        for (IndexData indexDN : this.indexList) {
            if (indexDN.containsMap(typeName) || indexDN.isMap(typeName)) {
                return indexDN.getLastVersionSchema(typeName);
            }
        }
        return -1;
    }

    public int getLastVersionIndex(String key) {
        for (IndexData indexDN : this.indexList) {
            if (indexDN.containsKey(key)) {
                return indexDN.getLastVersionSchema();
            }
        }
        return -1;
    }

    public void recoverSchema(IndexDirection indexDirection, Config appconfig, Boolean forceReloaded, Boolean continueIndexSchemaNotFound) throws SchemaRecoveryException, IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        //For echa index creates an schema
        Iterator<IndexData> iterador = indexList.iterator();
        while (iterador.hasNext()) {
            IndexData indexDN = iterador.next();
            try {
                indexDN.recoverSchema(indexDirection, appconfig, forceReloaded);
            } catch (IndexSchemaRecoveryException | InstantiationException |InvocationTargetException |NoSuchMethodException | IllegalAccessException |ClassNotFoundException    e) {
                if (!continueIndexSchemaNotFound) {
                    throw e;
                }
                iterador.remove();

            }
        }
    }


    public Map<String, String> getMapTypes(String mapName) throws SchemaRecoveryException {
        for (IndexData indexDN : this.indexList) {
            if (indexDN.hasTypesFor(mapName)) {
                return indexDN.getMapTypes();
            }
        }
        return null;
    }

    public String[] getFields(String mapName) {
        for (IndexData indexDN : this.indexList) {
            if (indexDN.containsMap(mapName)) {
                return indexDN.getFields(mapName);
            }
        }
        return null;
    }

    public String[] getKeys(String mapName) {
        for (IndexData indexDN : this.indexList) {
            if (indexDN.containsMap(mapName)) {
                return indexDN.getKeys();
            }
        }
        return null;
    }

    public List<String> getMaps() {
        List<String> indexListNames = new ArrayList<>();
        for (IndexData indexDN : this.indexList) {
            indexListNames.addAll(indexDN.getMaps());
        }
        return indexListNames;
    }

    public Collection<? extends String> getIndexMaps(String indexName) {
        for (IndexData indexDN : this.indexList) {
            if (indexDN.containsMap(indexName) || indexDN.isMap(indexName)) {
                return indexDN.getMaps();
            }
        }
        return null;
    }

    public String getIndexMapName(String mapName) {
        for (IndexData indexDN : this.indexList) {
            if (indexDN.containsMap(mapName) || indexDN.isMap(mapName)) {
                return indexDN.getIndexMapName();
            }
        }
        return null;
    }

    public String getPersistentMapName(String mapName) {
        for (IndexData indexDN : this.indexList) {
            if (indexDN.containsMap(mapName) || indexDN.isMap(mapName) || indexDN.getPaginationMap().equals(mapName)) {
                return indexDN.getPersistentMapName(mapName);
            }
        }
        return null;
    }

    public String getPaginationMap(String mapName) {
        for (IndexData indexDN : this.indexList) {
            if (indexDN.containsMap(mapName) || indexDN.isMap(mapName)) {
                return indexDN.getPaginationMap();
            }
        }
        return null;
    }

    public String getPrincipal(String mapName) {
        for (IndexData indexDN : this.indexList) {
            if (indexDN.containsMap(mapName) || indexDN.isMap(mapName)) {
                return indexDN.getPrincipal();
            }
        }
        return null;
    }


    public Schema getPaginationSchema(String mapName) {
        for (IndexData indexDN : this.indexList) {
            if (indexDN.containsMap(mapName)) {
                return indexDN.getPaginationSchema();
            }
        }
        return null;
    }

    public boolean isIndexable(String mapName) {
        for (IndexData indexDN : this.indexList) {
            if (indexDN.containsMap(mapName)) {
                return true;
            }
        }
        return false;
    }

    public String shadedMapName(String mapName) {
        for (IndexData indexDN : this.indexList) {
            String shadedName = indexDN.getOutofIndexName();
            if (shadedName != null) {
                return shadedName;
            }
        }

        return null;
    }

    public List<String> getIndexMapsByKey(String key) {

        for (IndexData indexDN : this.indexList) {


            if (indexDN.containsKey(key)) {
                return new ArrayList(indexDN.getMaps());
            }

        }
        return null;


    }

    public String getIndexMapNameByKey(String key) {
        for (IndexData indexDN : this.indexList) {


            if (indexDN.containsKey(key)) {
                return indexDN.getIndexMapName();
            }

        }
        return null;

    }

    public String getPaginationMapByKey(String key) {

        for (IndexData indexDN : this.indexList) {
            if (indexDN.containsKey(key)) {
                return indexDN.getPaginationMap();
            }
        }
        return null;
    }

    public Schema getPaginationSchemaByKey(String indexKey) {

        for (IndexData indexDN : this.indexList) {
            if (indexDN.containsKey(indexKey)) {
                return indexDN.getPaginationSchema();
            }
        }
        return null;

    }

    public List<String> getIndexKeys() {
        List<String> indexKeys = new ArrayList<>();

        for (IndexData indexDN : this.indexList) {
            indexKeys.add(indexDN.getKey());
        }

        return indexKeys;
    }

    public String getIndexKey(String mapName) {

        for (IndexData indexDN : this.indexList) {
            if (indexDN.containsMap(mapName)) {
                return indexDN.getKey();
            }
        }
        return null;
    }


}