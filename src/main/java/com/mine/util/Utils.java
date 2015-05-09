package com.mine.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class Utils {
	public static final byte[] convertObject2Byte(Object o) {
		if (o == null) {
			return null;
		}

		ByteArrayOutputStream result = new ByteArrayOutputStream();
		ObjectOutputStream out = null;
		try {
			out = new ObjectOutputStream(result);
			out.writeObject(o);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				out = null;
			}
		}
		return result.toByteArray();
	}
}
