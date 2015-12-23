package sg.edu.nyp.sit.svds.client.ida;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.StringTokenizer;

public class GaussJordan {
	public static final long serialVersionUID = 1L;
	
	   // swap()
	   // swap row i with row k
	   // pre: A[i][q]==A[k][q]==0 for 1<=q<j
	   static void swap(double[][] A, int i, int k, int j){
	      int m = A[0].length - 1;
	      double temp;
	      for(int q=j; q<=m; q++){
	         temp = A[i][q];
	         A[i][q] = A[k][q];
	         A[k][q] = temp;
	      }
	   }

	   // divide()
	   // divide row i by A[i][j]
	   // pre: A[i][j]!=0, A[i][q]==0 for 1<=q<j
	   // post: A[i][j]==1;
	   static void divide(double[][] A, int i, int j){
	      int m = A[0].length - 1;
	      //for(int q=j+1; q<=m; q++) A[i][q] /= A[i][j];
	      for(int q=j+1; q<=m; q++)
	    	  A[i][q] /= A[i][j];
	      A[i][j] = 1;
	   }

	   // eliminate()
	   // subtract an appropriate multiple of row i from every other row
	   // pre: A[i][j]==1, A[i][q]==0 for 1<=q<j
	   // post: A[p][j]==0 for p!=i
	   static void eliminate(double[][] A, int i, int j){
	      int n = A.length - 1;
	      int m = A[0].length - 1;
	      for(int p=1; p<=n; p++){
	         if( p!=i && A[p][j]!=0 ){
	            for(int q=j+1; q<=m; q++){
	               A[p][q] -= A[p][j]*A[i][q];
	            }
	            A[p][j] = 0;
	         }
	      }
	   }

	   // printMatrix()
	   // print the present state of Matrix A to file out
	   static void printMatrix(PrintWriter out, double[][] A){
	     int n = A.length - 1;
	     int m = A[0].length - 1;
	      for(int i=1; i<=n; i++){
	         for(int j=1; j<=m; j++) out.print(A[i][j] + "  ");
	         out.println();
	      }
	      out.println();
	      out.println();
	   }

	   // main()
	   // read input file, initialize matrix, perform Gauss-Jordan Elimination,
	   // and write resulting matrices to output file
	   public static void main(String[] args) throws IOException {
	      int n, m, i, j, k;
	      String line;
	      StringTokenizer st;

	      // check command line arguments, open input and output files
//	      if( args.length!=2 ){
//	         System.out.println("Usage: GaussJordan infile outfile");
//	         System.exit(1);
//	      }
	      
	      String strIn ="C:\\Users\\cylaw\\Documents\\Temp\\matrix_in.txt"; 
	      String strOut="C:\\Users\\cylaw\\Documents\\Temp\\matrix_out.txt";;
	    	  
	      BufferedReader in = new BufferedReader(new FileReader(strIn));
	      PrintWriter out = new PrintWriter(new FileWriter(strOut));

	      // read first line of input file
	      line = in.readLine();
	      st = new StringTokenizer(line);
	      n = Integer.parseInt(st.nextToken());
	      m = Integer.parseInt(st.nextToken());

	      // declare A to be of size (n+1)x(m+1) and do not use index 0
	      double[][] A = new double[n+1][m+1];

	      // read next n lines of input file and initialize array A
	      for(i=1; i<=n; i++){
	         line = in.readLine();
	         st = new StringTokenizer(line);
	         for(j=1; j<=m; j++){
	            A[i][j] = Double.parseDouble(st.nextToken());
	         }
	      }

	      // close input file
	      in.close();

	      // print array A to output file
	      printMatrix(out, A);

	      // perform Gauss-Jordan Elimination algorithm
	      i = 1;
	      j = 1;
	      while( i<=n && j<=m ){

	         //look for a non-zero entry in col j at or below row i
	         k = i;
	         while( k<=n && A[k][j]==0 ) k++;

	         // if such an entry is found at row k
	         if( k<=n ){

	            //  if k is not i, then swap row i with row k
	            if( k!=i ) {
	               swap(A, i, k, j);
	               printMatrix(out, A);
	            }

	            // if A[i][j] is not 1, then divide row i by A[i][j]
	            if( A[i][j]!=1 ){
	               divide(A, i, j);
	               printMatrix(out, A);
	            }

	            // eliminate all other non-zero entries from col j by subtracting from each
	            // row (other than i) an appropriate multiple of row i
	            eliminate(A, i, j);
	            printMatrix(out, A);
	            i++;
	         }
	         j++;
	      }

	      // print rank to output file
	      out.println("rank = " + (i-1));

	      // close output file
	      out.close();
	   }
}
