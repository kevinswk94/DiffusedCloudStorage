package sg.edu.nyp.sit.svds.client.gf2;

public class Gf2Matrix {
	public static final long serialVersionUID = 1L;
	
	int rows;
	int cols;
	int width;			/* number of bytes in each element */
	char[] values;

	public static final int UNDEFINED = -1;
	public static final int ROWWISE = 0;
	public static final int COLWISE = 1;

	int organisation = UNDEFINED;
	
	@Override
	public boolean equals(Object obj)
	{
		if(obj instanceof Gf2Matrix == false)
			return false;
		
		Gf2Matrix mat = (Gf2Matrix)obj;
		
		if(rows != mat.rows)
			return false;
		if(cols != mat.cols)
			return false;
		if(width!= mat.width)
			return false;
		if(organisation != mat.organisation)
			return false;

		if(values.length != mat.values.length)
			return false;
		
		for (int i = 0; i < values.length; i++) {
			if(values[i] != mat.values[i])
				return false;
		}

		return true;
	}

	public int gf2_matrix_offset_right (Gf2Matrix m) {

		if (m == null) 
			return 0;

		switch (m.organisation) {
		case Gf2Matrix.ROWWISE:
			return m.width;
		case Gf2Matrix.COLWISE:
			return m.rows * m.width;
		}
		return 0;
	}

	int gf2_matrix_offset_down (Gf2Matrix m) {

		if (m == null)
			return 0;

		switch (m.organisation) {
		case Gf2Matrix.ROWWISE:
			return m.cols * m.width;
		case Gf2Matrix.COLWISE:
			return m.width;
		}
		return 0;
	}

	Gf2Matrix gf2_identity_matrix (Gf2Matrix dest, int rows, int cols, int width, int org) {

		int   i,j,k;

		if (dest == null) {

			if (rows != cols) 
				return null;
			if ((rows <= 0) || (cols <= 0))
				return null;
			if ((width <= 0) || (width > 128))
				return null;
			if (org == 0)
				return null;

			dest = new Gf2Matrix();

			dest.values = new char[rows * cols * width];

			//dest.alloc_bits=FREE_BOTH;

			dest.rows         = rows;
			dest.cols         = cols;
			dest.width        = width;
			dest.organisation = org;

		} else {

			if (dest.rows != dest.cols)             
				return null;
			if (rows != 0  && (dest.rows != rows))        
				return null;
			if (cols != 0  && (dest.cols != rows))        
				return null;
			if (org != 0  && (dest.organisation != org)) 
				return null;
			if (width != 0 && (dest.rows != width))       
				return null;

			/* update local vars with values from struct */
			rows=dest.rows;
			cols=dest.cols;
			width=dest.width;
		}
		  /*
	    Blank matrix values, then set diagonal elements to 1.
	      Since the matrix is square, we don't need to distinguish between
	    ROWWISE/COLWISE organisation.
		   */
		//memset(dest->values, 0, rows * cols * width);
		for (int k2 = 0; k2 < dest.values.length; k2++) {
			dest.values[k2] = 0;
		}
		
		i=0; j=width-1;
		k=(rows + 1) * width;           /* offset of next diagonal */
		do {
			(dest.values)[j]=1;	      	 /* set low byte/bit */
			j+=k;
		} while (++i < rows);

		return dest;
	}
	
	/*
	  Some routines to help find the location of a given cell within the
	  block allocated to the matrix and otherwise navigate within that
	  block. All take account of the organisation setting for the matrix,
	  where relevant. The most efficient way to navigate a matrix would be
	  to write a routine which assumes a certain ROWWISE/COLWISE
	  organisation. The next best would be to call gf2_matrix_offset_right
	  and gf2_matrix_offset_down once in the code and then use them to
	  traverse to the right/down the matrix. The least efficient way would
	  be to call gf2_matrix_offset to calculate the address of the desired
	  row and colum.
	 */

	int gf2_matrix_row_size_in_bytes (Gf2Matrix m) {

		if (m == null) return 0;

		return (m.cols * m.width);
	}
	
	int gf2_matrix_col_size_in_bytes (Gf2Matrix m) {

		if (m == null) return 0;

		return (m.rows * m.width);
	}
	
}