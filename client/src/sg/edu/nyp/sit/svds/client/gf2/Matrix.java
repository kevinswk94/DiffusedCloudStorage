package sg.edu.nyp.sit.svds.client.gf2;

import sg.edu.nyp.sit.svds.exception.IDAException;

public class Matrix {
	public static final long serialVersionUID = 1L;
	
	public static void main(String[] args)
	{
		Matrix mat = new Matrix();
		int[][] values = new int[3][3];
		for (int i = 0; i < values.length; i++) {
			for (int j = 0; j < values[0].length; j++) {
				values[i][j] = i+j;
			}
		}
		mat.setValues(values);
		System.out.println(mat.toString());
		
		Matrix matTemp;
		try {
			for (int i = 0; i < 3; i++) {
				for (int j = 0; j < 3; j++) {
					matTemp = new Matrix();
					matTemp.setValues(mat.getCofactorElement(mat.values, i, j));
					System.out.println(matTemp.toString());
				}
			}
		}
		catch(IDAException ex) {
			System.out.println(ex.getMessage());
		}
		
	}
	
	@Override
	public String toString()
	{
		if(values == null)
			return "Empty matrix";
		
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < values.length; i++) {
			for (int j = 0; j < values[0].length; j++) {
				sb.append(values[i][j]);
				sb.append(" ");
			}
			sb.append("\n");
		}
		
		return sb.toString();
	}
	
	private int[][] values;

	public int[][] getValues() {
		return values;
	}

	public void setValues(int[][] values) {
		this.values = values;
	}
	
	int getDetOrder2(int[][] matrix)
	{
		GaloisField gf = GaloisField.getInstance();
		
		long l1 = gf.gf2_mul(8, matrix[0][0], matrix[1][1]); 
		long l2 = gf.gf2_mul(8, matrix[0][1], matrix[1][0]);
		return (int)(l1 - l2);
	}
	
	int getDeterminant(int[][] values) throws IDAException
	{
		if(values.length == 2) {
			return values[0][0] * values[1][1] - values[0][1] * values[1][0];
		}
		
		int rows = values.length;
		//int cols = values[0].length;
		int sign = 1;
		int col = 0;
		
		//using the first column
		int acc = 0;
		for (int i = 0; i < rows; i++) {
			acc += sign * values[i][col] * getDeterminant(getCofactorElement(values, i, col));
			sign *= -1; //toggle sign
		}
		
		return acc;
	}
	
	/*
	private int[][] getCofactorMatrix(int[][] values)
	{
//		int rows = values.length;
//		int cols = values[0].length;
//		
//		int sign = 1;
//		int[][] result = new int[rows][cols];
//		
//		for (int i = 0; i < rows; i++) {
//			for (int j = 0; j < cols; j++) {
//				result[i][j] = sign * 
//			}
//			
//		}
		
		return null;
		
	}
	*/
	
	
	private int[][] getCofactorElement(int[][] values, int row, int col)
		throws IDAException
	{
		if(values == null || values.length < 1)
			throw new IDAException("Matrix::getSubOrderMarix:Error in values parameter");
		
		int RowCount = values.length;
		int ColCount = values[0].length;
		
		int matRow = 0, matCol = 0;
		int[][] mat = new int[RowCount-1][ColCount-1];
		for (int i = 0; i < RowCount; i++) {
			if(i == row)
				continue;
			matCol = 0;
			for (int j = 0; j < ColCount; j++) {
				if(j == col)
					continue;
				mat[matRow][matCol++] = values[i][j];
			}
			matRow++;
		}
		
		return mat;
	}
}
