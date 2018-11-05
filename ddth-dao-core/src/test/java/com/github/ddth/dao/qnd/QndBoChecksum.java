package com.github.ddth.dao.qnd;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.github.ddth.dao.BaseBo;
import com.github.ddth.dao.BaseDataJsonFieldBo;

public class QndBoChecksum {
    private final static class MyBaseBo extends BaseBo {
        public Map<String, Object> getAttributes() {
            return attributeMap();
        }

        public void reset() {
            setAttributes((Map<String, Object>) null);
        }
    }

    private final static class MyBaseDataJsonFieldBo extends BaseDataJsonFieldBo {
        public Map<String, Object> getAttributes() {
            return attributeMap();
        }

        public void reset() {
            setAttributes((Map<String, Object>) null);
        }
    }

    public static void main(String[] args) {
        Random random = new Random(System.currentTimeMillis());
        List<Integer> data = Arrays.asList(1, 3, 5, 7, 9, 11, 13, 15, 17, 19);

        {
            MyBaseBo bo = new MyBaseBo();

            bo.reset();
            Collections.shuffle(data, random);
            System.out.println("Data: " + data);
            System.out.println(bo.calcChecksum() + ": " + bo.getAttributes());
            data.forEach(i -> bo.setAttribute("" + i, i));
            System.out.println(bo.calcChecksum() + ": " + bo.getAttributes());
            System.out.println("Json: " + bo.getAttributesAsJsonString());
            System.out.println();

            bo.reset();
            Collections.shuffle(data, random);
            System.out.println("Data: " + data);
            System.out.println(bo.calcChecksum() + ": " + bo.getAttributes());
            data.forEach(i -> bo.setAttribute("" + i, i));
            System.out.println(bo.calcChecksum() + ": " + bo.getAttributes());
            System.out.println("Json: " + bo.getAttributesAsJsonString());
            System.out.println();

            bo.reset();
            Collections.shuffle(data, random);
            System.out.println("Data: " + data);
            System.out.println(bo.calcChecksum() + ": " + bo.getAttributes());
            data.forEach(i -> bo.setAttribute("" + i, i));
            System.out.println(bo.calcChecksum() + ": " + bo.getAttributes());
            System.out.println("Json: " + bo.getAttributesAsJsonString());
            System.out.println();
        }

        System.out
                .println("======================================================================");

        {
            MyBaseDataJsonFieldBo bo = new MyBaseDataJsonFieldBo();

            bo.reset();
            Collections.shuffle(data, random);
            System.out.println("Data  : " + data);
            System.out.println(bo.calcChecksum() + ": " + bo.getAttributes());
            data.forEach(i -> bo.setAttribute("" + i, i));
            data.forEach(i -> bo.setDataAttr("" + i, i));
            System.out.println(bo.calcChecksum() + ": " + bo.getAttributes());
            System.out.println("Json  : " + bo.getAttributesAsJsonString());
            System.out.println("BoData: " + bo.getData());
            System.out.println();

            bo.reset();
            Collections.shuffle(data, random);
            System.out.println("Data  : " + data);
            System.out.println(bo.calcChecksum() + ": " + bo.getAttributes());
            data.forEach(i -> bo.setAttribute("" + i, i));
            data.forEach(i -> bo.setDataAttr("" + i, i));
            System.out.println(bo.calcChecksum() + ": " + bo.getAttributes());
            System.out.println("Json  : " + bo.getAttributesAsJsonString());
            System.out.println("BoData: " + bo.getData());
            System.out.println();

            bo.reset();
            Collections.shuffle(data, random);
            System.out.println("Data  : " + data);
            System.out.println(bo.calcChecksum() + ": " + bo.getAttributes());
            data.forEach(i -> bo.setAttribute("" + i, i));
            data.forEach(i -> bo.setDataAttr("" + i, i));
            System.out.println(bo.calcChecksum() + ": " + bo.getAttributes());
            System.out.println("Json  : " + bo.getAttributesAsJsonString());
            System.out.println("BoData: " + bo.getData());
            System.out.println();
        }
    }
}
