package com.github.ddth.dao.qnd;

import java.util.HashMap;
import java.util.Map;

import com.github.ddth.cacheadapter.cacheimpl.redis.RedisCacheFactory;
import com.github.ddth.dao.BaseDao;

public class QndBaseDaoCacheException {

    private static class MyDao extends BaseDao {
        private final static String CACHE_NAME = "QndBaseDaoCacheException.MyDao";
        private Map<String, Object> storage = new HashMap<>();

        public Object getBo(String id) {
            Object result = getFromCache(CACHE_NAME, id);
            if (result == null) {
                result = storage.get(id);
                putToCache(CACHE_NAME, id, result);
            }
            return result;
        }

        public void setBo(String id, Object value) {
            storage.put(id, value);
            removeFromCache(CACHE_NAME, id);
        }
    }

    public static void main(String[] args) {
        try (RedisCacheFactory cacheFactory = new RedisCacheFactory()) {
            cacheFactory.setRedisHostAndPort("localhost:6379");
            cacheFactory.init();

            try (MyDao dao = new MyDao()) {
                dao.setCacheFactory(cacheFactory);
                dao.init();

                System.out.println(dao.getBo("1"));
                dao.setBo("2", "myvalue-2");
                System.out.println(dao.getBo("2"));
            }
        }
    }
}
