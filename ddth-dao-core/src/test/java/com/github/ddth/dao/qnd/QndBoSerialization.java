package com.github.ddth.dao.qnd;

import com.github.ddth.dao.BaseBo;

public class QndBoSerialization {
	static class MyBo extends BaseBo {
		public String name() {
			return getAttribute("name", String.class);
		}

		public MyBo name(String name) {
			setAttribute("name", name);
			return this;
		}
	}

	public static void main(String[] args) {
		MyBo bo = new MyBo();
		bo.name("Nguyen Ba Thanh");
		System.out.println(bo);

		byte[] data1 = bo.toByteArray();
		System.out.println(bo.name("").fromByteArray(data1));

		String data2 = bo.toJson();
		System.out.println(bo.name("").fromJson(data2));

		// byte[] data1 = SerializationUtils.toByteArray(bo);
		// MyBo bo1 = (MyBo) SerializationUtils.fromByteArray(data1);
		// System.out.println(bo1);
		//
		// byte[] data2 = SerializationUtils.toByteArrayKryo(bo);
		// Object bo2 = SerializationUtils.fromByteArrayKryo(data2, MyBo.class);
		// System.out.println(bo2);
	}
}
