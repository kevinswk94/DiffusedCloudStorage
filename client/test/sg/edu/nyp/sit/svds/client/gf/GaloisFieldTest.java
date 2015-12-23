package sg.edu.nyp.sit.svds.client.gf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.Assert;
import org.junit.Test;

import sg.edu.nyp.sit.svds.client.gf.GaloisField;
import sg.edu.nyp.sit.svds.client.gf.GaloisFieldElement;
import sg.edu.nyp.sit.svds.client.gf.GaloisFieldPolynomial;

public class GaloisFieldTest {

	@Test
	/**
	 * Tests the GaloisField elements against the results obtained from 
	 * original C++ library
	 * The result is stored in file name c_out.dat in the same directory as
	 * this current class.
	 */
	public void testGaloisField() {

		int poly[] = { 1, 1, 1, 0, 0, 0, 0, 1, 1 };
		// unsigned int poly[9] = {1,0,0,0,1,1,0,1,1};

		GaloisField gf = new GaloisField(8, poly);
		//cylaw:The test should not be run in multi-threaded environment
		//otherwise, change StringBuilder to StringBuffer
		StringBuilder sb = new StringBuilder();

		// for (unsigned int k = 0; k < 10000; k++)
		for (int k = 0; k < 1; k++) {
			for (int i = 0; i < 256; i++) {
				for (int j = 0; j < 256; j++) {
					int symb;
					symb = gf.mul(i, j);
					sb.append(symb);
					sb.append(" ");
					symb = gf.div(i, j);
					sb.append(symb);
					sb.append(" ");
					symb = gf.exp(i, j);
					sb.append(symb);
					sb.append(" ");
				}
			}
		}
		
		try {
			Assert.assertEquals(getCppOrginalResult(), sb.toString());
		}
		catch(IOException ex) {
			Assert.fail("Cannot Run Test, error retreiving expected result");
		}
	}
	
	@Test
	public void testSimple()
	{
		int poly[] = { 1, 1, 1, 0, 0, 0, 0, 1, 1 };

		GaloisField gf = new GaloisField(8, poly);
		
		//0x88 = 10001000
		GaloisFieldElement[] gfe1 = new GaloisFieldElement[8];
		gfe1[0] = new GaloisFieldElement(gf, 0);
		gfe1[1] = new GaloisFieldElement(gf, 0);
		gfe1[2] = new GaloisFieldElement(gf, 0);
		gfe1[3] = new GaloisFieldElement(gf, 1);
		gfe1[4] = new GaloisFieldElement(gf, 0);
		gfe1[5] = new GaloisFieldElement(gf, 0);
		gfe1[6] = new GaloisFieldElement(gf, 0);
		gfe1[7] = new GaloisFieldElement(gf, 1);
		GaloisFieldPolynomial poly1 = new GaloisFieldPolynomial(gf, 7, gfe1); 
		
		//0x20 = 100000
		GaloisFieldElement[] gfe2 = new GaloisFieldElement[6];
		gfe2[0] = new GaloisFieldElement(gf, 0);
		gfe2[1] = new GaloisFieldElement(gf, 0);
		gfe2[2] = new GaloisFieldElement(gf, 0);
		gfe2[3] = new GaloisFieldElement(gf, 0);
		gfe2[4] = new GaloisFieldElement(gf, 0);
		gfe2[5] = new GaloisFieldElement(gf, 1);
		GaloisFieldPolynomial poly2 = new GaloisFieldPolynomial(gf, 5, gfe2);

		//111000011
		GaloisFieldElement[] gfe3 = new GaloisFieldElement[9];
		gfe3[0] = new GaloisFieldElement(gf, 1);
		gfe3[1] = new GaloisFieldElement(gf, 1);
		gfe3[2] = new GaloisFieldElement(gf, 0);
		gfe3[3] = new GaloisFieldElement(gf, 0);
		gfe3[4] = new GaloisFieldElement(gf, 0);
		gfe3[5] = new GaloisFieldElement(gf, 0);
		gfe3[6] = new GaloisFieldElement(gf, 1);
		gfe3[7] = new GaloisFieldElement(gf, 1);
		gfe3[8] = new GaloisFieldElement(gf, 1);
		GaloisFieldPolynomial poly3 = new GaloisFieldPolynomial(gf, 8, gfe3);
		
		GaloisFieldPolynomial polyMul = poly1.opMultiply(poly2);
		GaloisFieldPolynomial polyMod = polyMul.opMod(poly3);
		
		//Expected Result = 10101110
		GaloisFieldElement[] gfeResult = new GaloisFieldElement[8];
		gfeResult[0] = new GaloisFieldElement(gf, 0);
		gfeResult[1] = new GaloisFieldElement(gf, 1);
		gfeResult[2] = new GaloisFieldElement(gf, 1);
		gfeResult[3] = new GaloisFieldElement(gf, 1);
		gfeResult[4] = new GaloisFieldElement(gf, 0);
		gfeResult[5] = new GaloisFieldElement(gf, 1);
		gfeResult[6] = new GaloisFieldElement(gf, 0);
		gfeResult[7] = new GaloisFieldElement(gf, 1);
		GaloisFieldPolynomial polyExpected = new GaloisFieldPolynomial(gf, 7, gfeResult);
		
		Assert.assertEquals(polyMod.equals(polyExpected), true);
		
		
		
	}

	/**
	 * Retrieves the result output from C++ library as string
	 * Format is each result is separated by a blank " "
	 * @return String of all 256 x 256 results separated by a " "
	 * @throws IOException
	 */
	public String getCppOrginalResult() throws IOException
	{
		BufferedReader br = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("c_out.dat")));
		StringBuilder sb = new StringBuilder();
		String line = null;

		while ((line = br.readLine()) != null) {
			sb.append(line); //\n will be removed
		}

		br.close();
		
		System.out.println(sb.toString());
		return sb.toString();
	}
}
