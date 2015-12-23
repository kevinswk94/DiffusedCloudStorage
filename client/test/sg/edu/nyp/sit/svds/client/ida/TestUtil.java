package sg.edu.nyp.sit.svds.client.ida;

import static org.junit.Assert.fail;

import org.junit.Assert;
import org.junit.Test;

import sg.edu.nyp.sit.svds.client.gf2.GaloisField;
import sg.edu.nyp.sit.svds.exception.IDAException;

public class TestUtil {

	//@Test
	public void testConversionInteger() {
		byte[] ba;
		for (int i = Integer.MIN_VALUE; i < Integer.MAX_VALUE; i++) {
			ba = Util.integerToBytes(i);
			Assert.assertEquals(i, Util.bytesToInteger(ba));
		}
	}
	
	@Test
	public void testConversionShort()
	{
		byte[] ba;
		for (short i = Short.MIN_VALUE; i < Short.MAX_VALUE; i++) {
			ba = Util.shortToBytes(i);
			Assert.assertEquals(i, Util.bytesToShort(ba));
		}
	}
	
	@Test
	public void testInvertMatrix2()
	{
		try {
			int[][] values = new int[4][7]; //index 0 is not used
			values[1][1] = 1;
			values[1][2] = 1;
			values[1][3] = 1;
			values[1][4] = 1;
			values[1][5] = 0;
			values[1][6] = 0;
			
			values[2][1] = 1;
			values[2][2] = 2;
			values[2][3] = 4;
			values[2][4] = 0;
			values[2][5] = 1;
			values[2][6] = 0;
	
			values[3][1] = 2;
			values[3][2] = 5;
			values[3][3] = 2;
			values[3][4] = 0;
			values[3][5] = 0;
			values[3][6] = 1;
			
			int[][] result = Util.invertMatrixGf2(values);
			//Inverse Matrix
			System.out.println(Util.matrixToString(result));
			
			int[][] org_values = new int[3][3];
			org_values[0][0] = 3;
			org_values[0][1] = 2;
			org_values[0][2] = 1;
			org_values[1][0] = 4;
			org_values[1][1] = 1;
			org_values[1][2] = 3;
			org_values[2][0] = 2;
			org_values[2][1] = 5;
			org_values[2][2] = 2;
			
			//Check by muliplying inverse with orginal matrix, 
			//it should get identity
			result = matrixMulGf2(org_values, result);
			
			int[][] identity = new int[3][3];
			identity[0][0] = 1;
			identity[0][1] = 0;
			identity[0][2] = 0;
			identity[1][0] = 0;
			identity[1][1] = 1;
			identity[1][2] = 0;
			identity[2][0] = 0;
			identity[2][1] = 0;
			identity[2][2] = 1;
			
			Assert.assertEquals(true, compareMatrix(result, identity));
		}
		catch(IDAException ex)
		{
			fail(ex.getMessage());
		}
	}
	
	@Test
	public void testInvertMatrix()
	{
		try {
			int[][] values = new int[4][7]; //index 0 is not used
			values[1][1] = 3;
			values[1][2] = 2;
			values[1][3] = 1;
			values[1][4] = 1;
			values[1][5] = 0;
			values[1][6] = 0;
			
			values[2][1] = 4;
			values[2][2] = 1;
			values[2][3] = 3;
			values[2][4] = 0;
			values[2][5] = 1;
			values[2][6] = 0;
	
			values[3][1] = 2;
			values[3][2] = 5;
			values[3][3] = 2;
			values[3][4] = 0;
			values[3][5] = 0;
			values[3][6] = 1;
			
			int[][] result = Util.invertMatrixGf2(values);
			//Inverse Matrix
			System.out.println(Util.matrixToString(result));
			
			int[][] org_values = new int[3][3];
			org_values[0][0] = 3;
			org_values[0][1] = 2;
			org_values[0][2] = 1;
			org_values[1][0] = 4;
			org_values[1][1] = 1;
			org_values[1][2] = 3;
			org_values[2][0] = 2;
			org_values[2][1] = 5;
			org_values[2][2] = 2;
			
			//Check by muliplying inverse with orginal matrix, 
			//it should get identity
			result = matrixMulGf2(org_values, result);
			
			int[][] identity = new int[3][3];
			identity[0][0] = 1;
			identity[0][1] = 0;
			identity[0][2] = 0;
			identity[1][0] = 0;
			identity[1][1] = 1;
			identity[1][2] = 0;
			identity[2][0] = 0;
			identity[2][1] = 0;
			identity[2][2] = 1;
			
			Assert.assertEquals(true, compareMatrix(result, identity));
		}
		catch(IDAException ex)
		{
			fail(ex.getMessage());
		}
	}
	
	private boolean compareMatrix(int[][] m1, int[][] m2)
	{
		if(m1.length < 1)
			return false;
		if(m1.length != m2.length)
			return false;
		if(m2[0].length != m2[0].length)
			return false;
		
		for (int i = 0; i < m1.length; i++) {
			for (int j = 0; j < m1[0].length; j++) {
				if(m1[i][j] != m2[i][j])
					return false;
			}
		}
		
		return true;
	}
	
	private int[][] matrixMulGf2(int[][] mat1, int[][] mat2)
	{
		if(mat1.length != mat2[0].length)
			return null;

		int rows = mat1.length;
		int cols = mat1[0].length;
		
		int acc;
		int[][] result = new int[mat1.length][mat2[0].length];
		for (int r = 0; r < rows; r++) {
			for (int c = 0; c < mat2[0].length; c++) {
				
				acc = 0;
				for (int i = 0; i < cols; i++) {
					acc = acc ^ (int)GaloisField.getInstance().gf2_mul(8, mat1[r][i], mat2[i][c]);
				}
				result[r][c] = acc;
			}
		}
		
		return result;
	}
	
	@Test
	public void testGenerateIndependenceMatrix() 
	{
		int[][] mat = Util.generateIndependenceMatrix(10,7);
		
	}
	
}
