package sg.edu.nyp.sit.svds.client.gf;

public class GaloisFieldElement {
	public static final long serialVersionUID = 1L;
	
    private GaloisField gf;
    private int poly_value;

    public void opAssign(GaloisFieldElement gfe)
    {
       gf = gfe.gf;
       poly_value = gfe.poly_value;
    }

    public void opAssign(int v)
    {
       this.poly_value  = v & gf.size();
    }
    
    public void opPlusAndAssign(GaloisFieldElement gfe)
    {
       this.poly_value ^= gfe.poly_value;
    }
    
    public void opPlusAndAssign(int v)
    {
       this.poly_value ^= v;
    }
    
    public void opMinusAndAssign(GaloisFieldElement gfe)
    {
        opPlusAndAssign(gfe);
    }

    public void opMinusAndAssign(int v)
    {
       this.opPlusAndAssign(v);
    }

    public void opMultiplyAndAssign(GaloisFieldElement gfe)
    {
       this.poly_value = this.gf.mul(poly_value,gfe.poly_value);
    }

    public void opMultiplyAndAssign(int v)
    {
        poly_value = gf.mul(poly_value,v);
    }

    public void opDivideAndAssign(GaloisFieldElement gfe)
    {
        poly_value = gf.div(poly_value,gfe.poly_value);
    }

    public void opDivideAndAssign(int v)
    {
        poly_value = gf.div(poly_value,v);
    }
    
    public void opCaretAndAssign(int n)
    {
       this.poly_value = this.gf.exp(poly_value,n);
    }

	public boolean equals(GaloisFieldElement gfe)
    {
		return (
				(gf  == gfe.gf) && (poly_value == gfe.poly_value)
		);
    }

	public boolean equals(int v)
    {
        return (poly_value == v);
    }
    
    public boolean opLessThan(GaloisFieldElement gfe)
    {
       return (poly_value < gfe.poly_value);
    }

    public boolean opLessThan(int v)
    {
        return (poly_value < v);
    }

    public boolean opGreaterThan(GaloisFieldElement gfe)
    {
       return (poly_value > gfe.poly_value);
    }

    public boolean opGreaterThan(int v)
    {
        return (poly_value > v);
    }
    
    public int index()
    {
       return gf.index(poly_value);
    }

    public int poly()
    {
       return poly_value;
    }
    
    public GaloisField field()
    {
       return gf;
    }
    
    public int inverse()
    {
       return gf.inverse(poly_value);
    }

    //Constructor
    
	public GaloisFieldElement(GaloisField _gf, int v)
	{
		if (_gf != null)
		{
			this.gf = new GaloisField(_gf);
			poly_value = v & this.gf.size();
		}
		else
			poly_value = v;
	}

	public GaloisFieldElement(GaloisFieldElement gfe)
	{
		this.gf          = gfe.gf;
		this.poly_value  = gfe.poly_value;
	}
	
	public GaloisFieldElement opPlus(GaloisFieldElement a)
	{
		GaloisFieldElement e = new GaloisFieldElement(this);
		e.opPlusAndAssign(a);
		return e;
	}

	public GaloisFieldElement opMinus(GaloisFieldElement a)
	{
		GaloisFieldElement e = new GaloisFieldElement(this);
		e.opMinusAndAssign(a);
		return e;
	}

	public GaloisFieldElement opMultiply(GaloisFieldElement a)
	{
		GaloisFieldElement e = new GaloisFieldElement(this);
		e.opMultiplyAndAssign(a);
		return e;
	}
	
	public GaloisFieldElement opMultiply(int v)
	{
		GaloisFieldElement e = new GaloisFieldElement(this);
		e.opMultiplyAndAssign(v);
		return e;
	}
	
	public GaloisFieldElement opDivide(GaloisFieldElement a)
	{
		GaloisFieldElement e = new GaloisFieldElement(this);
		e.opDivideAndAssign(a);
		return e;
	}
	
	public GaloisFieldElement opCaret(int v)
	{
		GaloisFieldElement e = new GaloisFieldElement(this);
		e.opCaret(v);
		return e;
	}

	@Override
	public String toString()
	{
		return Integer.toString(poly_value);
	}

}
