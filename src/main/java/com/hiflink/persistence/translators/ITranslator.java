package com.hiflink.persistence.translators;

public interface ITranslator {
    public static ITranslator newTranslatorManager(String implementationPath) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class<?> cls = Class.forName(implementationPath, true, classLoader);
        return (ITranslator) cls.newInstance();

    }
}