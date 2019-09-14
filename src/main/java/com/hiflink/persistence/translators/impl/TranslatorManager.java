package com.hiflink.persistence.translators.impl;

import com.hiflink.persistence.translators.ITranslator;
import com.hiflink.persistence.translators.ITranslatorManager;

import java.util.HashMap;
import java.util.Map;

public class TranslatorManager implements ITranslatorManager {

    HashMap<String, ITranslator> translatorMap;

    protected TranslatorManager() {
        translatorMap = new HashMap<>();
    }

    @Override
    public ITranslator getTranslator(String name) {
        if (this.translatorMap.containsKey(name)) {
            return translatorMap.get(name);
        }
        return null;
    }

    @Override
    public void appendTranslator(String name, ITranslator translator) {

        if (!this.translatorMap.containsKey(name)) {
            this.translatorMap.put(name, translator);
        }
    }

    @Override
    public void removeTranslator(String name) {

        if (this.translatorMap.containsKey(name)) {
            this.translatorMap.remove(name);
        }
    }

    @Override
    public Map<String, Object> translateToIndex(Map<String, Object> mapSearch) {
        //Recorre la lista de keys recuperando de cada tipo de traductor la información correspondiente a cada una de ellas
        return mapSearch;
    }

    @Override
    public Map<String, String> translateFromIndex(Map<String, String> mapSearch) {
        return mapSearch;
    }

    @Override
    public String translateField(String indexField) {
        return indexField;
    }

    @Override
    public Object translateValue(String indexField, Object recordValue) {
        return (recordValue instanceof String) ? String.valueOf(recordValue).toUpperCase() : recordValue;
    }
}
