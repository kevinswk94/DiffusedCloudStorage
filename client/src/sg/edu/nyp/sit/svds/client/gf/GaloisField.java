package sg.edu.nyp.sit.svds.client.gf;

public class GaloisField {
	public static final long serialVersionUID = 1L;
	
	int power;
	int field_size;
	int prim_poly_hash;
	int[] alpha_to;    // aka exponential or anti-log
	int[] index_of;    // aka log
	int[] mul_inverse; // multiplicative inverse
	int[][] mul_table;
	int[][] div_table;
	int[][] exp_table;

	private static final int GFERROR = -1;

	public GaloisField() {
		alpha_to       = new int [1];
		index_of       = new int [1];
		mul_inverse    = new int [1];
		mul_table      = new int [1][1];
		div_table      = new int [1][1];
		exp_table      = new int [1][1];
		prim_poly_hash = 0;
	}

	public GaloisField(int pwr, int[] primitive_poly)
	{
		power = pwr;
		field_size = (1 << power) -1;

		alpha_to    = new int [field_size + 1];
		index_of    = new int [field_size + 1];

		mul_table   = new int [(field_size + 1)][1];
		div_table   = new int [(field_size + 1)][1];
		exp_table   = new int [(field_size + 1)][1];
		mul_inverse = new int [1];

		prim_poly_hash = 0xAAAAAAAA;

		for (int i = 0; i < power; i++)
		{
			prim_poly_hash += ((i & 1) == 0) ? (  (prim_poly_hash <<  7) ^ primitive_poly[i] ^ (prim_poly_hash >> 3)) :
				(~((prim_poly_hash << 11) ^ primitive_poly[i] ^ (prim_poly_hash >> 5)));
		}

		generate_field(primitive_poly);
	}

	public GaloisField(GaloisField gf)
	{
		power          = gf.power;
		field_size     = gf.field_size;
		prim_poly_hash = gf.prim_poly_hash;
		alpha_to       = new int[field_size + 1];
		index_of       = new int[field_size + 1];

		for (int i = 0; i < gf.alpha_to.length; i++) {
			alpha_to[i] = gf.alpha_to[i];
			index_of[i] = gf.index_of[i];
		}
	}

	public boolean equals(GaloisField gf)
	{
		return (
				(this.power == gf.power) && (this.prim_poly_hash == gf.prim_poly_hash)
		) ;
	}



	public int index(int value)
	{
		return index_of[value];
	}

	public int alpha(int value)
	{
		return alpha_to[value];
	}

	public int size()
	{
		return field_size;
	}

	public int pwr()
	{
		return power;
	}

	public int add(int a, int b)
	{
		return (a ^ b);
	}

	public int sub(int a, int b)
	{
		return (a ^ b);
	}

	public int mul(int a, int b)
	{
		if ((a == 0) || (b == 0))
			return 0;
		else
			return alpha_to[fast_modulus(index_of[a] + index_of[b])];
	}

	public int div(int a, int b)
	{
		if ((a == 0) || (b == 0))
			return 0;
		else
			return alpha_to[fast_modulus(index_of[a] - index_of[b] + field_size)];
	}

	/**
	 * @param a
	 * @param n
	 * @return
	 */
	 public int exp(int a, int n)
	{
		if (a != 0)
		{
			if (n < 0)
			{
				int b = n;
				while(b < 0) b += field_size;// b could be negative
				if (b == 0)
					return 1;
				return alpha_to[fast_modulus(index_of[a] * b)];
			}
			else if (n == 0)
				return 1;
			else
				return alpha_to[fast_modulus(index_of[a] * n)];
		}
		else
			return 0;
	}

	 public int inverse(int val)
	 {
		 return alpha_to[fast_modulus(field_size - index_of[val])];
	 }

	 int fast_modulus(int x)
	 {
		 while (x >= (int)field_size)
		 {
			 x -= (int)field_size;
			 x  = (x >> power) + (x & (int)field_size);
		 }

		 return x;
	 }

	 public int gen_mul(int a, int b)
	 {
		 if ((a == 0) || (b == 0))
			 return 0;
		 else
			 return alpha_to[fast_modulus(index_of[a] + index_of[b])];
	 }

	 int gen_div(int a, int b)
	 {
		 if ((a == 0) || (b == 0))
			 return 0;
		 else
			 return alpha_to[fast_modulus(index_of[a] - index_of[b] + field_size)];
	 }
	 
	 int gen_exp(int a, int n)
	 {
		 if (a != 0)
		 {
			 if (n == 0)
				 return 1;
			 else
				 return alpha_to[fast_modulus(index_of[a] * n)];
		 }
		 else
			 return 0;
	 }

	 public int gen_inverse(int val)
	 {
		 return alpha_to[fast_modulus(field_size - index_of[val])];
	 }

	 

	 void generate_field(int[] prim_poly)
	 {
		 /*
          Note: It is assumed that the degree of the primitive
                polynomial will be equivelent to the m value as
                in GF(2^m)
		  */

		 /*
          need to update using stanford method for prim-poly generation.
		  */
		 int mask = 1;

		 alpha_to[power] = 0;

		 //cylaw: ^ with the bits that are on in the prim_poly
		 for (int i = 0; i < power; i++)
		 {
			 alpha_to[i] = mask;
			 index_of[alpha_to[i]] = i;

			 if (prim_poly[i] != 0)
			 {
				 alpha_to[power] ^= mask;
			 }

			 mask <<= 1;
		 }

		 index_of[alpha_to[power]] = power;

		 mask >>= 1;

		 for (int i = power + 1; i < field_size; i++)
		 {
			 if (alpha_to[i - 1] >= mask)
				 alpha_to[i] = alpha_to[power] ^ ((alpha_to[i - 1] ^ mask) << 1);
			 else
				 alpha_to[i] = alpha_to[i - 1] << 1;

			 //System.out.println("index_of:%d=%d\n", alpha_to[i], i); 

			 index_of[alpha_to[i]] = i;
		 }

		 index_of[0] = GFERROR;
		 alpha_to[field_size] = 1;
	 }
	 
	 @Override
	 public String toString()
	 {
		 String str = "";
	      for(int i = 0; i < field_size + 1; i++)
	      {
	         str += i;
	         str += "\t";
	         str += alpha_to[i];
	         str += "\t";
	         str += index_of[i];
	         str += "\n";
	      }

	      return str;		 
	 }
	 
}
