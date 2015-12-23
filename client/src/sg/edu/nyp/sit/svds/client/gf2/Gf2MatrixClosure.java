package sg.edu.nyp.sit.svds.client.gf2;

public class Gf2MatrixClosure {
	public static final long serialVersionUID = 1L;
	
	  //gf2_matrix_callback fp;       /* function callback */

	  char  u1_type;		/* must match names in union below */
	  int   u1_many;		/* for pointer types, how many? */
	  Gf2Polymorphic u1;

	  char  u2_type;		/* must match names in union below */
	  int   u2_many;		/* for pointer types, how many? */
	  Gf2Polymorphic u2;
}