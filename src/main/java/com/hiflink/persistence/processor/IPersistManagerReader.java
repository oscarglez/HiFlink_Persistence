package com.hiflink.persistence.processor;

import com.hiflink.common.storage.Exceptions.MaxRecordsExcedeedException;
import com.hiflink.common.storage.Exceptions.NotRecognicedLockerOwnerException;
import com.hiflink.common.storage.sorting.IPaginationSet;
import com.hiflink.common.storage.sorting.SortingEntityCollection;
import com.hiflink.persistence.analyzer.IndexDirection;
import com.hiflink.persistence.exceptions.IndexesUpdateException;
import com.hiflink.persistence.exceptions.MalformedIANFileException;
import com.hiflink.persistence.exceptions.ReindexIsLockException;
import com.hiflink.persistence.exceptions.SchemaRecoveryException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import scala.Tuple2;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public interface IPersistManagerReader {

    // public IPersistManagerReader newInstanceReader(String configPath) throws Exception
    public static IPersistManagerReader newPersistManagerReader(String configPath, boolean continueIndexSchemaNotFound, String mode, String cloudPath) throws MalformedIANFileException, IOException, SchemaRecoveryException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        return new PersistManager(configPath, IndexDirection.consumer, continueIndexSchemaNotFound,mode,cloudPath);
    }
    public Schema getIndexationSchema(String typeName);

    Schema getIndexationSchemaByKey(String key);

    public Schema getSchema(String typeName) throws IndexesUpdateException, ReindexIsLockException, IOException, SchemaRecoveryException;

    public int getLastVersionSchema(String avroSchemaName) throws IndexesUpdateException, ReindexIsLockException;

    public int getLastVersionSchemaIndex(String indexKey) throws IndexesUpdateException, ReindexIsLockException;


    public Collection<GenericRecord> search(String typeName, String sqlPredicate, String[] recoveredFields) throws Exception;
//
//    /** Ejecuta el predicado SQL sobre sobre el typeName solicitado
//     *
//     * @param typeName {String} type sobre el que ejecutar
//     * @param sqlPredicate {String} Predicado SQL
//     * @param queryFields {List<String>} Campos de búsqueda de la query
//     *
//     * @return {Collection<GenericRecord>}
//     * @throws Exception
//     */
//    @Deprecated
//    public Collection<GenericRecord> search(String typeName, String sqlPredicate, List<String> queryFields) throws Exception, IndexesUpdateException, ReindexIsLockException;

    List<String> getIndexMapsByKey(String key);

    List<String> getIndexDisplayNamesByKey(String key);

    String getIndexationSchemaNameByKey(String key);

    public Collection<GenericRecord> search(String typeName, Map<String, Object> sqlPredicate, Optional<SortingEntityCollection> sortingEntityCollection) throws IndexesUpdateException, ReindexIsLockException, IndexesUpdateException, ReindexIsLockException, NotRecognicedLockerOwnerException, SchemaRecoveryException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException;

    public Tuple2<IPaginationSet,Collection<GenericRecord>> searchPaginated(String typeName, Map<String, Object> mapSearch, Optional<IPaginationSet> paginationSet, Optional<SortingEntityCollection> sortingEntityCollection) throws MaxRecordsExcedeedException, IndexesUpdateException, ReindexIsLockException, NotRecognicedLockerOwnerException, SchemaRecoveryException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException;

//    @Deprecated
//    public Tuple2<IPaginationSet,Collection<GenericRecord>> searchPaginated(String typeName, String sqlPredicateSearch, List<String> queryFields, Optional<IPaginationSet> paginationSet, Optional<SortingEntityCollection> sortingEntityCollection) throws MaxRecordsExcedeedException, IndexesUpdateException, ReindexIsLockException, NotRecognicedLockerOwnerException, SchemaRecoveryException;

//    @Deprecated
//    public Collection<GenericRecord> search(GenericRecord emptyGenericRecord, String sqlPredicate) throws Exception;
//    @Deprecated
//    public Collection<GenericRecord> search(GenericRecord emptyGenericRecord, String sqlPredicate, String[] recoveredFields) throws Exception;
//    @Deprecated
//    public Collection<GenericRecord> search(GenericRecord emptyGenericRecord, HashMap<String, String> sqlPredicate) throws Exception;

    //TODO
    public GenericRecord searchByID(String typeName, String id)throws IndexesUpdateException, ReindexIsLockException;
    //TODO
//    @Deprecated
//    public Collection<GenericRecord> searchByID(String typeName, String[] id) throws Exception;


    public void reviewIndexes(boolean continueIndexSchemaNotFound) throws MalformedIANFileException, IOException, SchemaRecoveryException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException;

    public Map<String, String> getMapTypes(String indexKey);


    public List<String> getIndexKeys();

    public String getKeyFromRecord(String mapName, GenericRecord genericRecord);

    public String getIndexKey(String mapName);
}