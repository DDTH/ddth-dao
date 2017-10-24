package com.github.ddth.dao.qnd;

public class QndDynamicArg {

    public static void test(Object... args) {
        System.out.println(args.length);
    }

    public static void main(String[] args) {
        test();
        test((Object[]) null);
    }
}
