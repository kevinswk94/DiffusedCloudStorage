package sg.edu.nyp.sit.svds.client.ida;

public class InfoDispersalFactory {
	public static final long serialVersionUID = 1L;
	
	//private static final IInfoDispersal INSTANCE = new DummyInfoDispersalImpl();
	private static final IInfoDispersal INSTANCE = new RabinImpl2();
	
	public static IInfoDispersal getInstance() {
		return INSTANCE;
	}

	private InfoDispersalFactory() {

	}
}
