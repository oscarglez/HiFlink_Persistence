package com.hiflink.persistence.translators;

import com.hiflink.persistence.translators.impl.TranslatorManager;

import java.util.Map;

public interface ITranslatorManager {
    public static ITranslatorManager newTranslatorManager(String implementationPath) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class<?> cls = Class.forName(implementationPath, true, classLoader);
        return (ITranslatorManager) cls.newInstance();

    }

    public ITranslator getTranslator(String name);

    public void appendTranslator(String name, ITranslator translator);

    public void removeTranslator(String name);

    public Map<String, Object> translateToIndex(Map<String, Object> mapSearch);

    public Map<String, String> translateFromIndex(Map<String, String> mapSearch);

    public String translateField(String indexField);

    public Object translateValue(String indexField, Object recordValue);

}
