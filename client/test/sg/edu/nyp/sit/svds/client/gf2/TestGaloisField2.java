package sg.edu.nyp.sit.svds.client.gf2;

import junit.framework.Assert;

import org.junit.Test;

import sg.edu.nyp.sit.svds.client.gf2.GaloisField;

public class TestGaloisField2 {

	@Test
	public void testGf2_div() {
		int a = 0x53;
		int b = 0xCA;
		
		GaloisField gf2 = new GaloisField();
		int c = (int)gf2.gf2_div(8, a, b);
		Assert.assertEquals(181, c);
	}

	@Test
	public void testGf2_mul() {
		int a = 0x53;
		int b = 0xCA;
		
		GaloisField gf2 = new GaloisField();
		int c = (int)gf2.gf2_mul(8, a, b);
		Assert.assertEquals(1, c);
	}

	@Test
	public void testGf2_inv() {
		GaloisField gf2 = new GaloisField();
		int c = (int)gf2.gf2_inv(8, 0x53);
		Assert.assertEquals(202, c);
	}

	@Test
	public void testGf2_pow() {
		int a = 0x53;
		int b = 0xCA;
		
		GaloisField gf2 = new GaloisField();
		long c = gf2.gf2_pow(8, a, b);
		
		Assert.assertEquals(85, c);

	}

}
