package sg.edu.nyp.sit.svds.client.gf2;

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.junit.Test;

import sg.edu.nyp.sit.svds.exception.IDAException;

public class MatrixTest {

	@Test
	public void testGetDeterminant() {
		int[][] values = new int[3][3];
		values[0][0] = 1;
		values[0][1] = 3;
		values[0][2] = 2;
		values[1][0] = 4;
		values[1][1] = 1;
		values[1][2] = 3;
		values[2][0] = 2;
		values[2][1] = 5;
		values[2][2] = 2;
		
		Matrix mat = new Matrix();
		mat.setValues(values);
		
		try {
			Assert.assertEquals(17, mat.getDeterminant(values));
		}
		catch(IDAException ex)
		{
			fail(ex.getMessage());
		}
		
		values[0][0] = 1;
		values[0][1] = 2;
		values[0][2] = 0;
		values[1][0] = -1;
		values[1][1] = 1;
		values[1][2] = 1;
		values[2][0] = 1;
		values[2][1] = 2;
		values[2][2] = 3;
		mat.setValues(values);

		try {
			Assert.assertEquals(9, mat.getDeterminant(values));
		}
		catch(IDAException ex)
		{
			fail(ex.getMessage());
		}
	}

}
