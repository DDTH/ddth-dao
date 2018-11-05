package com.github.ddth.dao.qnd;

public class QndException {

    private static void methodA() {
        throw new RuntimeException();
    }

    private static void methodB() {
        try {
            methodA();
        } catch (RuntimeException e) {
            Exception _dummy = new Exception();
            e.setStackTrace(_dummy.getStackTrace());
            throw e;
        }
    }

    public static void main(String[] args) {
        // methodA();
        methodB();
    }

}
