package sg.edu.nyp.sit.svds.client.gf;

import java.util.Vector;

public class GaloisFieldPolynomial {
	public static final long serialVersionUID = 1L;
	
	private GaloisField gf;
	private Vector<GaloisFieldElement> poly = new Vector<GaloisFieldElement>();
	
	public GaloisFieldPolynomial(GaloisField _gf)
	{
		this.gf = _gf;
		poly.clear();
	}

	public GaloisFieldPolynomial(GaloisField _gf, int size, GaloisFieldElement[] gfe)
	{
		gf = _gf;
		//poly.resize(size + 1); C++
		poly.setSize(size+1);
		
		if (gfe != null)
		{
			for(int i = 0; i <= size; i++)
			{
				poly.set(i, gfe[i]);
			}
		}
		else
		{
			for(int i = 0; i < poly.size(); i++)
			{
				poly.set(i, new GaloisFieldElement(gf,0));
			}
		}
	}

   public GaloisFieldPolynomial(GaloisFieldPolynomial polynomial)
   {
      gf   = polynomial.gf;
      poly = polynomial.poly;
   }
   
   public GaloisFieldPolynomial(GaloisFieldElement gfe)
   {
      gf = gfe.field();
      poly.clear();
      poly.add(gfe);
   }
   
   public boolean valid()
   {
      return (poly.size() > 0);
   }
   
   public int deg()
   {
      return (poly.size() - 1);
   }
   
   public GaloisField field()
   {
      return gf;
   }
   
   public void set_degree(int x)
   {
	   resize(x - 1);
   }
   
   private void resize(int x)
   {
	   int oldsize = poly.size();

	   if(x < oldsize) {
		   poly.setSize(x);
		   return;
	   }

	   for (int i = oldsize; i < x; i++) {
		   poly.add(new GaloisFieldElement(gf, 0));
	   }
   }

   public void opAssign(GaloisFieldPolynomial polynomial)
   {
      if (this.equals(polynomial))
        return;

      gf   = polynomial.gf;
      poly = polynomial.poly;

   }
   
   public void opAssign(GaloisFieldElement gfe)
   {
      poly.clear();
      gf = gfe.field();
      poly.add(gfe);
   }
   
   public void opPlusAndAssign(GaloisFieldPolynomial polynomial)
   {
      if (gf == polynomial.gf)
      {
         if (poly.size() < polynomial.poly.size())
         {
            int j = 0;
            for (int i = 0; i < poly.size(); i++)
            {
               poly.get(i).opPlusAndAssign(polynomial.poly.get(j++));
            }

            for ( ; j < polynomial.poly.size(); j++)
            {
            	poly.add(polynomial.poly.get(j));
            }
         }
         else
         {
            int i = 0;
            for (int j = 0; j < polynomial.poly.size(); j++)
            {
            	poly.get(i++).opPlusAndAssign(polynomial.poly.get(j));
            }
         }

         simplify(this);
      }

   }
   
   public void opPlusAndAssign(GaloisFieldElement gfe)
   {
      poly.get(0).opPlusAndAssign(gfe);
   }

   public void opMinusAndAssign(GaloisFieldPolynomial gfe)
   {
      this.opPlusAndAssign(gfe);
   }
   
   public void opMinusAndAssign(GaloisFieldElement gfe)
   {
      poly.get(0).opMinusAndAssign(gfe);
   }
   
   public void opMultiplyAndAssign(GaloisFieldPolynomial polynomial)
   {
      if (gf == polynomial.gf)
      {
         GaloisFieldPolynomial product = new GaloisFieldPolynomial(gf, deg() + polynomial.deg() + 1, null);
         GaloisFieldElement gfei, gfej, gfe;

         for (int  i= 0; i < poly.size(); i++)
         {
            for (int j = 0; j < polynomial.poly.size(); j++)
            {
            	gfei = poly.get(i);
            	gfej = polynomial.poly.get(j);
            	gfe = gfei.opMultiply(gfej);
               product.poly.get(i + j).opPlusAndAssign(gfe);
            }
         }

         simplify(product);
         poly = product.poly;
      }
   }

   public void opMultiplyAndAssign(GaloisFieldElement gfe)
   {
      if (gf == gfe.field())
      {
         for(int i = 0; i < poly.size(); i++)
         {
            poly.get(i).opMultiplyAndAssign(gfe);
         }
      }
   }
   
   public void opDivideAndAssign(GaloisFieldPolynomial divisor)
   {
      if (
          (gf            ==    divisor.gf) &&
          (deg()         >= divisor.deg()) &&
          (divisor.deg() >=             0)
         )
      {
          GaloisFieldPolynomial  quotient = new GaloisFieldPolynomial(gf, deg() - divisor.deg() + 1, null);
          GaloisFieldPolynomial  remainder = new GaloisFieldPolynomial(gf, divisor.deg() - 1, null);

         for(int i = deg(); i >= 0; i--)
         {
            if (i <= (int)quotient.deg())
            {
            	
            	
               quotient.opSet(i, remainder.opGet(remainder.deg()).opDivide(divisor.opGet(divisor.deg())));

               for(int j = remainder.deg(); j > 0; j--)
               {
                  remainder.opSet(j, remainder.opGet(j - 1).opPlus((quotient.opGet(i).opMultiply(divisor.opGet(j)))));
               }

               remainder.opSet(0, poly.get(i).opPlus((quotient.opGet(i).opMultiply(divisor.opGet(0)))));
            }
            else
            {
               for(int j = remainder.deg(); j > 0; j--)
               {
            	   remainder.opSet(j, remainder.opGet(j-1));
               }
               remainder.opSet(0, poly.get(i));
            }
         }

         simplify(quotient);

         poly = quotient.poly;
      }

   }

   public void opDivideAndAssign(GaloisFieldElement gfe)
   {
      if (gf == gfe.field())
      {
         for (int i = 0;  i < poly.size(); i++)
         {
            poly.get(i).opDivideAndAssign(gfe);
         }
      }
   }

   public void opModAndAssign(GaloisFieldPolynomial divisor)
   {
      if (
          (gf            ==    divisor.gf) &&
          (deg()         >= divisor.deg()) &&
          (divisor.deg() >=             0)
         )
      {
         GaloisFieldPolynomial quotient = new GaloisFieldPolynomial(gf, deg() - divisor.deg() + 1, null);
         GaloisFieldPolynomial remainder = new GaloisFieldPolynomial(gf, divisor.deg() - 1, null);

         for(int i = deg(); i >= 0; i--)
         {
            if (i <= (int)quotient.deg())
            {
            	quotient.opSet(i, remainder.opGet(remainder.deg()).opDivide(divisor.opGet(divisor.deg())));

               for(int j = remainder.deg(); j > 0; j--)
               {
                  remainder.opSet(j, remainder.opGet(j-1).opPlus(quotient.opGet(i).opMultiply(divisor.opGet(j))));
               }

               remainder.opSet(0, poly.get(i).opPlus(quotient.opGet(i).opMultiply(divisor.opGet(0))));
               
            }
            else
            {
               for(int j = remainder.deg(); j > 0; j--)
               {
            	   remainder.opSet(j, remainder.opGet(j-1));
               }
               remainder.opSet(0, poly.get(i));
            }
         }

         simplify(remainder);
         poly = remainder.poly;
      }
   }
   
   public void opModAndAssign(int power)
   {
      if (poly.size() >= power)
    	  resize(power);
      
      simplify(this);
   }

   public void opCaretAndAssign(int n)
   {
      GaloisFieldPolynomial result = new GaloisFieldPolynomial(this);

      for (int i = 0; i < n; i++)
      {
         result.opMultiplyAndAssign(this);
      }

      this.opAssign(result);

   }

   public void opInsertAndAssign(int n)
   {
      if (poly.size() > 0)
      {
         int initial_size = poly.size();
         resize(poly.size() + n);

         for(int i = initial_size - 1; i >= 0; i--)
         {
            poly.set(i + n, poly.get(i));
         }

         for(int i = 0; i < n; i++)
         {
            poly.set(i, null);
         }
      }
   }

   public void opRemoveAndAssign(int n)
   {
      if (n <= poly.size())
      {
         for(int i = 0;  i <= deg() - n; i++)
         {
            poly.set(i, poly.get(i + n));
         }

         resize(poly.size() - n);
      }
      else if (n >= deg() + 1)
      {
    	  resize(0);
      }
   }
   
   public GaloisFieldElement opGet(int term)
   {
      assert(term < poly.size());
      return poly.get(term);
   }

   public void opSet(int term, GaloisFieldElement e)
   {
      poly.set(term, e);
   }
   
   public GaloisFieldElement opBracket(GaloisFieldElement value)
   {
      GaloisFieldElement result = new GaloisFieldElement(gf,0);

      if (poly.size() > 0)
      {
         result = poly.get(poly.size() - 1);
         for(int i = poly.size() - 2; ((int)i) >= 0; i--)
         {
        	 result.opAssign(poly.get(i).opPlus(result.opMultiply(value)));
         }
      }
      return result;
   }
   
   public GaloisFieldElement opBracket(int value)
   {
	   return new GaloisFieldElement(gf, value);
      //return (*this)(GaloisFieldElement(gf,value));
   }
   
   public boolean equals(GaloisFieldPolynomial polynomial)
   {
      if (gf.equals(polynomial.gf))
      {
         if (poly.size() != polynomial.poly.size())
           return false;
         else
         {
            for (int i = 0; i < poly.size(); i++)
            {
               //cylaw: if (poly.get(i) != polynomial.poly.get(i))
            	if (poly.get(i).poly() != polynomial.poly.get(i).poly())
                 return false;
            }
            return true;
         }
      }
      else
        return false;
   }

   public boolean opNotEquals(GaloisFieldPolynomial polynomial)
   {
      return !(equals(polynomial));
   }

   public GaloisFieldPolynomial derivative()
   {
      if (this.poly.size() > 1)
      {
         GaloisFieldPolynomial deriv = new GaloisFieldPolynomial(gf,deg(), null);
         for (int i = 0; i < poly.size() - 1; i++)
         {
            if (((i + 1) & 1) == 1)
            	deriv.poly.get(i).opAssign(poly.get(i + 1));
            else
            	deriv.poly.set(i, null);
              //deriv.poly[i] = 0;
         }
         simplify(deriv);
         return deriv;
      }
      return new GaloisFieldPolynomial(gf,0, null);
   }
   
   public void simplify()
   {
      simplify(this);
   }

   public void simplify(GaloisFieldPolynomial polynomial)
   {
      if (poly.size() > 0)
      {
         int last = polynomial.poly.size() - 1;

         while((last >= 0) && (polynomial.poly.size() > 0))
         {
            if (polynomial.poly.get(last) == null)
              polynomial.poly.remove(poly.size()-1); //remove last element
            else
              break;

            last = polynomial.poly.size() - 1;
         }
      }
   }
   
   public GaloisFieldPolynomial opPlus(GaloisFieldPolynomial b)
   {
      GaloisFieldPolynomial result = new GaloisFieldPolynomial(this);
      result.opPlusAndAssign(b);
      return result;
   }
   
   public GaloisFieldPolynomial opPlus(GaloisFieldElement b)
   {
      GaloisFieldPolynomial result = new GaloisFieldPolynomial(this);
      result.opPlusAndAssign(b);
      return result;
   }

   public GaloisFieldPolynomial opPlus(int b)
   {
	   GaloisFieldElement gfe = new GaloisFieldElement(field(), b);
	   GaloisFieldPolynomial p = new GaloisFieldPolynomial(this);
	   return p.opPlus(gfe);
   }
   
   public GaloisFieldPolynomial opMinus(GaloisFieldPolynomial a, GaloisFieldPolynomial b)
   {
      GaloisFieldPolynomial result = new GaloisFieldPolynomial(this);
      result.opMinusAndAssign(b);
      return result;
   }

   public GaloisFieldPolynomial opMinus(GaloisFieldElement b)
   {
      GaloisFieldPolynomial result = new GaloisFieldPolynomial(this);
      result.opMinusAndAssign(b);
      return result;
   }
   
   public GaloisFieldPolynomial opMinus(int b)
   {
	   GaloisFieldElement gfe = new GaloisFieldElement(this.field(), b);
	   GaloisFieldPolynomial p = new GaloisFieldPolynomial(this);
	   return p.opMinus(gfe);
   }

   public GaloisFieldPolynomial opMultiply(GaloisFieldPolynomial b)
   {
      GaloisFieldPolynomial result = new GaloisFieldPolynomial(this);
      result.opMultiplyAndAssign(b);
      return result;
   }

   public GaloisFieldPolynomial opMultiply(GaloisFieldElement a)
   {
      GaloisFieldPolynomial result = new GaloisFieldPolynomial(this);
      result.opMultiplyAndAssign(a);
      return result;
   }
   
   public GaloisFieldPolynomial opDivide(GaloisFieldPolynomial b)
   {
      GaloisFieldPolynomial result = new GaloisFieldPolynomial(this);
      result.opDivideAndAssign(b);
      return result;
   }

   public GaloisFieldPolynomial opDivide(GaloisFieldElement b)
   {
      GaloisFieldPolynomial result = new GaloisFieldPolynomial(this);
      result.opDivide(b);
      return result;
   }
   
   public GaloisFieldPolynomial opMod(GaloisFieldPolynomial b)
   {
      GaloisFieldPolynomial result = new GaloisFieldPolynomial(this);
      result.opModAndAssign(b);
      return result;
   }

   public GaloisFieldPolynomial opMod(int power)
   {
      GaloisFieldPolynomial result = new GaloisFieldPolynomial(this);
      result.opModAndAssign(power);
      return result;
   }
   
   public GaloisFieldPolynomial opCaret(int n)
   {
      GaloisFieldPolynomial result = new GaloisFieldPolynomial(this);
      result.opCaretAndAssign(n);
      return result;
   }
   
   public GaloisFieldPolynomial gcd(GaloisFieldPolynomial b)
   {
      if ((field()) == (b.field()))
      {
         if ((!valid()) && (!b.valid())) 
        	 return null;
         
         if (!valid()) return b;
         if (!b.valid()) return this;

         GaloisFieldPolynomial x = this.opMod(b);
         GaloisFieldPolynomial y = b;
         GaloisFieldPolynomial z = x;

         while ((z = y.opMod(x)).valid())
         {
            y = x;
            x = z;
         }
         return x;
      }
      else
        return null;
   }
   
   @Override
   public String toString()
   {
	   StringBuffer sb = new StringBuffer();
       for (int i = 0; i < this.poly.size(); i++)
       {
          sb.append(this.poly.get(i).poly());
          sb.append(" ");
          sb.append("x^");
          sb.append(i);
          sb.append(((i != (this.deg())) ? " + " : ""));
       }
       
       return sb.toString();
   }

}
