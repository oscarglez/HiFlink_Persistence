package com.hiflink.persistence.processor;

import com.hiflink.common.storage.Exceptions.CleaningException;
import com.hiflink.common.storage.Exceptions.NotRecognicedLockerOwnerException;
import com.hiflink.common.storage.IClientStorage;
import com.hiflink.common.storage.sorting.IPaginationSet;
import com.hiflink.common.storage.sorting.SortingEntityCollection;
import com.hiflink.persistence.analyzer.IndexDirection;
import com.hiflink.common.storage.searching.ISearchParser;
import com.hiflink.persistence.exceptions.*;
import com.hiflink.persistence.index.IndexManager;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import scala.Tuple2;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class PersistManager implements IPersistManagerWriter, IPersistManagerReader {


    private String configFilePath;
    private Log logger = LogFactory.getLog(PersistManager.class);
    private IndexManager indexManager;
    IClientStorage iClientStorage;
    private IndexDirection indexDirection;
    private boolean indexUpdaterLocker = false;
    private boolean reindexProcessLocker = false;
    private boolean authorizedReindexation = false;
    private String mode;
    private String cloudPath;


    protected PersistManager(String configFilePath, IndexDirection indexDirection, boolean continueIndexSchemaNotFound, String mode, String cloudPath) throws MalformedIANFileException, IOException, SchemaRecoveryException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        this.configFilePath = configFilePath;
        this.indexDirection = indexDirection;
        this.mode = mode;
        this.cloudPath = cloudPath;
        indexManager = new IndexManager(configFilePath, indexDirection, continueIndexSchemaNotFound, mode, cloudPath);


        //IndexManager has to connect with hazelcast, create new maps supported by stratus structure
        iClientStorage = IClientStorage.newClientForStorage(indexManager.getStorageInstanceName(), configFilePath, mode, cloudPath);
        iClientStorage.connect();

        List<String> indexMapNames = indexManager.getIndexMaps();

        for (String mapName : indexMapNames) {
            this.iClientStorage.appendVirtualStore(mapName, this.indexManager.getPersistentMapName(mapName));
        }
    }


    public Schema getIndexationSchema(String typeName) {
        return indexManager.getIndexationSchema(typeName);
    }


    @Override
    public Schema getIndexationSchemaByKey(String key) {
        return indexManager.getIndexationSchemaByKey(key);
    }

    public Schema getSchema(String typeName) throws IndexesUpdateException, ReindexIsLockException, IOException, SchemaRecoveryException {
        if (!indexUpdaterLocker && !reindexProcessLocker) {
            return indexManager.getSchema(typeName);
        } else {
            if (indexUpdaterLocker) {
                throw new IndexesUpdateException("Update indexes data process is launched. Wait a few seconds and restart your process");
            } else if (reindexProcessLocker) {
                throw new ReindexIsLockException("Reindex process is launched. Wait a few seconds and restart your process");
            }
        }

        return null;

    }


    public int getLastVersionSchema(String typeName) throws IndexesUpdateException, ReindexIsLockException {
        if (!indexUpdaterLocker && !reindexProcessLocker) {
            return indexManager.getLastVersion(typeName);
        } else {
            if (indexUpdaterLocker) {
                throw new IndexesUpdateException("Update indexes data process is launched. Wait a few seconds and restart your process");
            } else if (reindexProcessLocker) {
                throw new ReindexIsLockException("Reindex process is launched. Wait a few seconds and restart your process");
            }
        }

        return -1;
    }


    public int getLastVersionSchemaIndex(String key) throws IndexesUpdateException, ReindexIsLockException {
        if (!indexUpdaterLocker && !reindexProcessLocker) {
            return indexManager.getLastVersionIndex(key);
        } else {
            if (indexUpdaterLocker) {
                throw new IndexesUpdateException("Update indexes data process is launched. Wait a few seconds and restart your process");
            } else if (reindexProcessLocker) {
                throw new ReindexIsLockException("Reindex process is launched. Wait a few seconds and restart your process");
            }
        }

        return -1;
    }


    @Override
    public List<String> getIndexMapsByKey(String key) {
        return indexManager.getIndexMapsByKey(key);
    }

    @Override
    public List<String> getIndexDisplayNamesByKey(String key) {


        Collection<? extends String> indexMapNames = indexManager.getIndexMapsByKey(key);

        String currentIndex = indexManager.getIndexMapNameByKey(key);
        String paginationName = indexManager.getPaginationMapByKey(key);


        ArrayList<String> reindexableCollections = new ArrayList<>();
        for (String indexMapName : indexMapNames) {
            if (!indexMapName.toLowerCase().equals(currentIndex.toLowerCase())
                    && !indexMapName.toLowerCase().equals(paginationName.toLowerCase())) {

                reindexableCollections.add(indexMapName);
            }
        }

        return reindexableCollections;
    }

    @Override
    public String getIndexationSchemaNameByKey(String key) {
        return indexManager.getIndexMapNameByKey(key);
    }


    //------------------------ Persist Writer Region  ------------------------//
    public boolean put(GenericRecord genericRecord) throws IndexesUpdateException, ReindexIsLockException {
        return put(genericRecord, false);
    }


    private boolean put(GenericRecord genericRecord, boolean isAuthorizedReindexation) throws IndexesUpdateException, ReindexIsLockException {
        if (!indexUpdaterLocker && (!reindexProcessLocker || (reindexProcessLocker && isAuthorizedReindexation))) {
            boolean transactionFinihed = false;
            String mapName;
            try {
                mapName = genericRecord.getSchema().getName();
                //Get data in order to save sended record
                String key = indexManager.getKey(mapName, genericRecord).toUpperCase();
                this.iClientStorage.save(mapName, key, genericRecord);

                transactionFinihed = putOnIndex(genericRecord, mapName, key, isAuthorizedReindexation);
            } catch (Exception e) {
                logger.error("Save generic record error.", e);
                transactionFinihed = false;
            }

            return transactionFinihed;
        } else {
            if (indexUpdaterLocker) {
                throw new IndexesUpdateException("Update indexes data process is launched. Wait a few seconds and restart your process");
            } else if (reindexProcessLocker) {
                throw new ReindexIsLockException("Reindex process is launched. Wait a few seconds and restart your process");
            }
        }
        return false;
    }

    private boolean putOnIndex(GenericRecord genericRecord, String mapName, String key, boolean isAuthorizedReindexation) throws IndexesUpdateException, ReindexIsLockException {
        if (!indexUpdaterLocker && (!reindexProcessLocker || (reindexProcessLocker && isAuthorizedReindexation))) {
            boolean transactionFinihed = false;
            try {
                if (indexManager.isIndexable(mapName)) {
                    //Get data in order to save index
                    String indexMap = indexManager.getIndexMapName(mapName);
                    GenericRecord indexRecord;
                    Object oldData = this.iClientStorage.searchByKey(indexMap, key);
                    GenericRecord oldIndexRecord = null;
                    if (oldData != null) {
                        oldIndexRecord = (GenericRecord) oldData;
                    }
                    indexRecord = indexManager.getGenericRecord(mapName, genericRecord, Optional.ofNullable(oldIndexRecord));

                    iClientStorage.save(indexMap, key, indexRecord, isAuthorizedReindexation ? Optional.of(this) : Optional.empty());
                }
                transactionFinihed = true;
            } catch (Exception e) {
                logger.error("Save generic record error on index.", e);
                transactionFinihed = false;
            }

            return transactionFinihed;
        } else {
            if (indexUpdaterLocker) {
                throw new IndexesUpdateException("Update indexes data process is launched. Wait a few seconds and restart your process");
            } else if (reindexProcessLocker) {
                throw new ReindexIsLockException("Reindex process is launched. Wait a few seconds and restart your process");
            }
        }
        return false;
    }

    //------------------------ EOPWR  ------------------------//

    //------------------------ Persist Reader Region  ------------------------//
    public Collection<GenericRecord> search(String indexKey, Map<String, Object> mapSearch, Optional<SortingEntityCollection> sortingEntityCollection) throws
            IndexesUpdateException, ReindexIsLockException, SchemaRecoveryException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (!indexUpdaterLocker && !reindexProcessLocker) {
            ISearchParser parser = ISearchParser.newSearchParser(indexManager.getSearcherInstanceName(),indexKey);

            String indexMap = indexManager.getIndexMapNameByKey(indexKey);
            Map<String, Object> mapSearchTranslated = indexManager.translate(mapSearch);
            Map<String, String> indexTypes = indexManager.getMapTypes(indexMap);

            //TODO: Paginación y predicados

            Collection<GenericRecord> result = this.iClientStorage.search(indexMap, parser.parse(mapSearchTranslated, indexTypes), sortingEntityCollection);
            return result;
        } else {
            if (indexUpdaterLocker) {
                throw new IndexesUpdateException("Update indexes data process is launched. Wait a few seconds and restart your process");
            } else if (reindexProcessLocker) {
                throw new ReindexIsLockException("Reindex process is launched. Wait a few seconds and restart your process");
            }
        }
        return null;
    }

    public Tuple2<IPaginationSet, Collection<GenericRecord>> searchPaginated(String indexKey, Map<String, Object> mapSearch, Optional<IPaginationSet> paginationSet, Optional<SortingEntityCollection> sortingEntityCollection) throws
            IndexesUpdateException, ReindexIsLockException, SchemaRecoveryException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (!indexUpdaterLocker && !reindexProcessLocker) {
            ISearchParser parser = ISearchParser.newSearchParser(indexManager.getSearcherInstanceName(), indexKey);

            String indexMap = indexManager.getIndexMapNameByKey(indexKey);
            String paginationMap = indexManager.getPaginationMapByKey(indexKey);
            Schema paginationSchema = indexManager.getPaginationSchemaByKey(indexKey);
            Map<String, Object> mapSearchTranslated = indexManager.translate(mapSearch);
            Map<String, String> indexTypes = indexManager.getMapTypes(indexMap);

            return this.iClientStorage.searchWithToken(indexMap, parser.parse(mapSearchTranslated, indexTypes), paginationMap, paginationSchema, paginationSet, sortingEntityCollection);
        } else {
            if (indexUpdaterLocker) {
                throw new IndexesUpdateException("Update indexes data process is launched. Wait a few seconds and restart your process");
            } else if (reindexProcessLocker) {
                throw new ReindexIsLockException("Reindex process is launched. Wait a few seconds and restart your process");
            }
        }
        return null;
    }


//    public Tuple2<IPaginationSet, Collection<GenericRecord>> searchPaginated(String typeName, String
//            sqlPredicateSearch, List<String> queryFields, Optional<IPaginationSet> paginationSet, Optional<SortingEntityCollection> sortingEntityCollection) throws
//            IndexesUpdateException, ReindexIsLockException, NotRecognicedLockerOwnerException, SchemaRecoveryException {
//        if (!indexUpdaterLocker && !reindexProcessLocker) {
//            String indexMap = indexManager.getIndexMapName(typeName);
//            String paginationMap = indexManager.getPaginationMap(typeName);
//            Schema paginationSchema = indexManager.getPaginationSchema(typeName);
//
//            //Perdir al IndexManager el campo key
//            String keyField =indexManager.getIndexKey(typeName);
//            SQLPredicateParser parser = new SQLPredicateParser(keyField);
//
//            String sqlPredicate = parser.parse(sqlPredicateSearch, queryFields);
//
//            System.out.println("sqlPredicate = [" + sqlPredicate + "]");
//
//
//            Map<String, String> indexTypes = indexManager.getMapTypes(indexMap);
//
//            return this.iClientStorage.searchWithToken(indexMap, sqlPredicate, paginationMap, paginationSchema, paginationSet, sortingEntityCollection);
//        } else {
//            if (indexUpdaterLocker) {
//                throw new IndexesUpdateException("Update indexes data process is launched. Wait a few seconds and restart your process");
//            } else if (reindexProcessLocker) {
//                throw new ReindexIsLockException("Reindex process is launched. Wait a few seconds and restart your process");
//            }
//        }
//        return null;
//    }


    public GenericRecord searchByID(String typeName, String id) throws IndexesUpdateException, ReindexIsLockException {
        if (!indexUpdaterLocker && !reindexProcessLocker) {
            Object oldData = this.iClientStorage.searchByKey(typeName, id);
            GenericRecord oldIndexRecord = null;
            if (oldData != null) {
                oldIndexRecord = (GenericRecord) oldData;
            }
            return oldIndexRecord;

        } else {
            if (indexUpdaterLocker) {
                throw new IndexesUpdateException("Update indexes data process is launched. Wait a few seconds and restart your process");
            } else if (reindexProcessLocker) {
                throw new ReindexIsLockException("Reindex process is launched. Wait a few seconds and restart your process");
            }
        }
        return null;
    }

    public Collection<GenericRecord> searchByID(String typeName, String[] id) throws Exception {

        throw new Exception("Not implemented method yet.");
    }


    public Collection<GenericRecord> search(String typeName, String sqlPredicate, String[] recoveredFields) throws
            Exception {

        throw new Exception("Not implemented method yet.");
    }

//    public Collection<GenericRecord> search(String typeName, String sqlPredicate, List<String> queryFields) throws
//            Exception, IndexesUpdateException, ReindexIsLockException {
//        if (!indexUpdaterLocker && !reindexProcessLocker) {
//
//            //Perdir al IndexManager el campo key
//            String keyField = "GLCS";
//            SQLPredicateParser parser = new SQLPredicateParser(keyField);
//
//            sqlPredicate = parser.parse(sqlPredicate, queryFields);
//
//            System.out.println("sqlPredicate = [" + sqlPredicate + "]");
//
//            String indexMap = indexManager.getIndexMapName(typeName);
//
//            Collection<GenericRecord> result = this.iClientStorage.search(indexMap, sqlPredicate, null);
//
//            return result;
//
//        } else {
//            if (indexUpdaterLocker) {
//                throw new IndexesUpdateException("Update indexes data process is launched. Wait a few seconds and restart your process");
//            } else if (reindexProcessLocker) {
//                throw new ReindexIsLockException("Reindex process is launched. Wait a few seconds and restart your process");
//            }
//        }
//        return null;
//
//    }

    public Collection<GenericRecord> search(GenericRecord emptyGenericRecord, String sqlPredicate) throws Exception {

        throw new Exception("Not implemented method yet.");
    }

    public Collection<GenericRecord> search(GenericRecord emptyGenericRecord, String sqlPredicate, String[]
            recoveredFields) throws Exception {
        throw new Exception("Not implemented method yet.");
    }

    public Collection<GenericRecord> search(GenericRecord
                                                    emptyGenericRecord, HashMap<String, String> sqlPredicate) throws Exception {
        throw new Exception("Not implemented method yet.");
    }

    //------------------------ Physical Index Review------------------------//

    @Override
    public void reviewIndexes(boolean continueIndexSchemaNotFound) throws MalformedIANFileException, IOException, SchemaRecoveryException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        try {
            indexUpdaterLocker = true;
            logger.info("START RELOADING INDEXES");
            IndexManager reloadedIndexManager = new IndexManager(configFilePath, this.indexDirection, true, continueIndexSchemaNotFound, mode, cloudPath);
            this.indexManager = reloadedIndexManager;

            logger.info("RELOADING MAPS RELOADING INDEXES");

            List<String> indexMapNames = indexManager.getIndexMaps();

            for (String mapName : indexMapNames) {
                this.iClientStorage.appendVirtualStore(mapName, this.indexManager.getPersistentMapName(mapName));
            }
            logger.info("END OF RELOADING INDEXES");
        } catch (Exception e) {
            throw e;
        } finally {
            indexUpdaterLocker = false;
        }

    }

    public void launchReindexProces(String indexName, Optional<String> principalName) throws UndefinedPrincipalReindexCollectionException, MalformedIANFileException, IOException, SchemaRecoveryException, CleaningException, NotRecognicedLockerOwnerException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        //Search by index
        //Analyze maps
        //Remove all data from maps which support index
        //For each collection starting by client DNA, recover data from collections and update it
        try {
            reindexProcessLocker = true;
            logger.info("START REINDEX PROCESS");
            //Gets persistReader
            IPersistManagerReader persistManagerReader = IPersistManagerReader.newPersistManagerReader(configFilePath, false, mode, cloudPath);

            /**Get all data for reindexation process
             * 1- Map collection to reindex
             * 2- Index map name. It will be cleaned
             * 3- Recover pagination name in order to remove from collection list
             * 4- Get principal name in order to use a data list as base list
             * 5- If principal isn't defined throws an exception
             * 6- Create a new list with reindex collections
             */
            Collection<? extends String> indexMapNames = indexManager.getIndexMaps(indexName);
            String currentIndex = indexManager.getIndexMapName(indexName);
            String paginationName = indexManager.getPaginationMap(indexName);
            String principal = principalName.orElse(indexManager.getPrincipal(indexName));

            if (principal == null || (principal != null && principal.isEmpty())) {
                throw new UndefinedPrincipalReindexCollectionException("Principal collection as reindex base has to be setted");
            }

            ArrayList<String> reindexableCollections = new ArrayList<>();
            for (String indexMapName : indexMapNames) {
                if (!indexMapName.toLowerCase().equals(currentIndex.toLowerCase())
                        && !indexMapName.toLowerCase().equals(paginationName.toLowerCase())
                        && !indexMapName.toLowerCase().equals(principal.toLowerCase())) {
                    reindexableCollections.add(indexMapName);
                }
            }
            /**
             * After recover all properties
             * 1- Remove all index data
             * 2- Go throught the principal collection
             * 3- Here a benchmark in needed
             */
            if (reindexSecuential(currentIndex, principal, reindexableCollections)) {
                logger.info("Reindex process has finished OK");
            } else {
                logger.info("Reindex process has finished KO");
            }
            logger.info("END OF REINDEX PROCESS");
        } finally {
            reindexProcessLocker = false;
        }


    }


    private boolean reindexSecuential(String currentIndex, String principal, ArrayList<String> reindexableCollections) throws CleaningException, NotRecognicedLockerOwnerException {
        /**
         * Delete data from index
         */
        ArrayList<String> lockableList = new ArrayList<>();
        String lockpassPhrase = "locking";
        boolean reindexationSucessfully = true;
        try {
            lockableList.addAll(reindexableCollections);
            lockableList.add(principal);
            lockableList.add(currentIndex);

            this.iClientStorage.lockMaps(this, lockableList);


            this.iClientStorage.clean(currentIndex, Optional.of(this));
            Collection<GenericRecord> principalCollection = this.iClientStorage.getFullCollection(principal, Optional.of(this));
            String mapName = ((List<GenericRecord>) principalCollection).get(0).getSchema().getName();

            //String key = indexManager.getKey(principal, ((List<GenericRecord>) principalCollection).get(0));

            for (GenericRecord genericRecord : principalCollection) {
                try {
                    String key = indexManager.getKey(principal, genericRecord);
                    this.putOnIndex(genericRecord, genericRecord.getSchema().getName(), key, true);
                    for (String reindexableMapsCollection : reindexableCollections) {
                        GenericRecord newGenericRecord = (GenericRecord) this.iClientStorage.searchByKey(reindexableMapsCollection, key);
                        if (newGenericRecord != null) {
                            this.putOnIndex(newGenericRecord, newGenericRecord.getSchema().getName(), key, true);
                        }
                    }
                } catch (IndexesUpdateException e) {
                    logger.info("Reindex failed", e);
                    reindexationSucessfully = false;
                } catch (ReindexIsLockException e) {
                    logger.info("Reindex failed", e);
                    reindexationSucessfully = false;
                }
            }

        } finally {
            this.iClientStorage.unlockMaps(this, lockableList);
        }
        return reindexationSucessfully;
    }

    private boolean reindexFull(String currentIndex, String principal, ArrayList<String> reindexableCollections) throws CleaningException {
        boolean reindexationSucessfully = true;
        return reindexationSucessfully;
    }
    //------------------------ EOPIR------------------------//


    @Override
    public Map<String, String> getMapTypes(String indexKey) {
        Map<String, String> indexTypes = null;
        String indexMap = indexManager.getIndexMapNameByKey(indexKey);
        try {
            indexTypes = indexManager.getMapTypes(indexMap);
        } catch (SchemaRecoveryException e) {
            e.printStackTrace();
        }

        return indexTypes;
    }


    @Override
    public List<String> getIndexKeys() {
        return this.indexManager.getIndexKeys();
    }


    @Override
    public String getKeyFromRecord(String mapName, GenericRecord genericRecord) {
        return this.indexManager.getKey(mapName, genericRecord);
    }


    @Override
    public String getIndexKey(String mapName) {
        return this.indexManager.getIndexKey(mapName);
    }


}
