package com.hiflink.persistence.processor;

import com.hiflink.common.storage.Exceptions.CleaningException;
import com.hiflink.common.storage.Exceptions.NotRecognicedLockerOwnerException;
import com.hiflink.persistence.analyzer.IndexDirection;
import com.hiflink.persistence.exceptions.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface IPersistManagerWriter {


    public static IPersistManagerWriter newInstanceWriter(String configPath, boolean continueIndexSchemaNotFound, String mode, String cloudPath) throws MalformedIANFileException, IOException, SchemaRecoveryException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        return new PersistManager(configPath, IndexDirection.producer, continueIndexSchemaNotFound,mode,cloudPath);
    }
    public boolean put(GenericRecord genericRecord) throws IndexesUpdateException, ReindexIsLockException;

    public Schema getSchema(String typeName) throws Exception, IndexesUpdateException, ReindexIsLockException;


    public void reviewIndexes(boolean continueIndexSchemaNotFound) throws MalformedIANFileException, IOException, SchemaRecoveryException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException;

    public void launchReindexProces(String indexName, Optional<String> principalName) throws UndefinedPrincipalReindexCollectionException, MalformedIANFileException, IOException, SchemaRecoveryException, CleaningException, NotRecognicedLockerOwnerException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException;





    List<String> getIndexMapsByKey(String key);

    List<String> getIndexDisplayNamesByKey(String key);

    String getIndexationSchemaNameByKey(String key);

    public Map<String, String> getMapTypes(String indexKey);

    public Schema getIndexationSchema(String typeName);

    Schema getIndexationSchemaByKey(String key);

    public int getLastVersionSchema(String avroSchemaName) throws IndexesUpdateException, ReindexIsLockException;

    public int getLastVersionSchemaIndex(String indexKey) throws IndexesUpdateException, ReindexIsLockException;


    public List<String> getIndexKeys();

    public String getKeyFromRecord(String mapName, GenericRecord genericRecord);


    public String getIndexKey(String mapName);
}