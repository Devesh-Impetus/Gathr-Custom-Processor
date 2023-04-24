package com.yourcompany.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;
import java.math.BigInteger;


public class SaltGenerator implements Serializable
{
	private static final long serialVersionUID = 6203162777272252311L;
	//private static Log logger = LogFactory.getLog(SaltGenerator.class);


	/**
	 * <b>Description:</b> Provides Salt prefix as per the provided AAA value.
	 *
	 * @param saltInputString
	 * @return Salt
	 * @version 1.0
	 * @since 11-11-2016
	 */
	public static Character getSalt(String saltInputString) {
		Character salt = null;

		if (saltInputString != null && !saltInputString.isEmpty()) {
			salt = new Character('\0');
			int lenght = 0;
			StringBuilder sb = new StringBuilder();
			char[] letters = saltInputString.toCharArray();
			for (char ch : letters) {
				sb.append((byte) ch);
			}
			BigInteger bi1 = new BigInteger(sb.toString());
			lenght = Math.abs(bi1.intValue());

			int len = String.valueOf(lenght).length();
			int mod = Math.abs(lenght % len);

			//logger.debug("Salt Prefix ====> " + mod + " for Input Salt String " + saltInputString);
			if (mod > 0 && mod <= 2) {
				salt = '#';
			} else if (mod > 2 && mod <= 4) {
				salt = '%';
			} else if (mod > 4 && mod <= 6) {
				salt = '@';
			} else if (mod > 6 && mod <= 8) {
				salt = '!';
			} else if (mod > 8 && mod <= 10) {
				salt = '~';
			} else {
				salt = '^';
			}
		}

		return salt;

	}


	/**
	 * <b>Description:</b> Provides Salt prefix as per the provided AAA value.
	 *
	 * @param saltInputString
	 * @return Salt
	 * @version 1.1
	 * @since 12-09-2018
	 * @Descreption getSalt method will take input as string and return a single value char or null.
	 * caller have check the not null value values.
	 */
	public static Character getSalt(String saltInputString, int totalNumberOfSalts) {
		Character salt = null;

		if (saltInputString != null && !saltInputString.isEmpty()) {
			salt = new Character('\0');
			int total = 0;
			char[] letters = saltInputString.toCharArray();

			for (char ch : letters) {
				total=total+((byte) ch);

			}

			int mod = Math.abs(total % totalNumberOfSalts);
			//	logger.debug("Salt Prefix ====> " + mod + " for Input Salt String " + saltInputString);
			if (mod > 0 && mod <= 1) {
				salt = '#';
			} else if (mod > 1 && mod <= 2) {
				salt = '%';
			} else if (mod > 2 && mod <= 3) {
				salt = '@';
			} else if (mod > 3 && mod <= 4) {
				salt = '!';
			} else if (mod > 4 && mod <= 5) {
				salt = '~';
			} else if (mod > 5 && mod <= 6) {
				salt = '&';
			} else if (mod > 6 && mod <= 7) {
				salt = '*';
			} else if (mod > 7 && mod <= 8) {
				salt = '(';
			} else if (mod > 8 && mod <= 9) {
				salt = ')';
			} else if (mod > 9 && mod <= 10) {
				salt = '+';
			} else if (mod > 10 && mod <= 11) {
				salt = '=';
			} else {
				salt = '^';
			}
		}

		return salt;

	}

	/**
	 * <b>Description:</b> Reverses the Domain name value and returns the same.
	 *
	 * @param domainName
	 * @return reverseDomainName
	 * @version 1.0
	 * @since 11-11-2016
	 */
	public static String reverseDomainName(String domainName) {
		if (domainName == null || domainName.isEmpty())
			return domainName;

		String components[] = domainName.split("\\.");
		// Collections.reverse(components);

		StringBuilder result = new StringBuilder(domainName.length());
		for (int i = components.length - 1; i > 0; i--) {
			result.append(components[i]).append(".");
		}
		return result.append(components[0]).toString();

	}


	public static void main(String[] args) {
		//Test1

		String []alpha={"A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Z"};
		for(int i=0;i<alpha.length;i++){
			System.out.println("Salt for "+alpha[i]+" : "+getSalt(alpha[i],12));
		}


		//Test2
		//System.out.println(getSalt("",12));

		//Test3
		for(int i=0;i<alpha.length;i++){
			System.out.println("Salt for "+alpha[i]+" : "+getSalt(alpha[i]+i,12));
		}
		//test 4
		System.out.println("Test4 : " + getSalt("dakkjdasjdladjd;asdad;lkasd;lad;A;DKA';LD;'DASD65SA564D54SD4GSD54G5Sn4G54DSG54DS5G45DS4G5D4G4",12));
		//test 5
		System.out.println("Test5 : " + getSalt("3232248426",12));
	}
}
