package com.epam.bigdata.q3.task9;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class Main {
	 public static void main(String[] args) throws IOException {
	        if (args.length < 1) {
	            throw new IllegalArgumentException("Must have either 'producer' or 'consumer' as argument");
	        }
	        switch (args[0]) {
	            case "producer":
	                Producer.main(args);
	                break;
	            default:
	                throw new IllegalArgumentException("Don't know how to do " + args[0]);
	        }
	    }
}
