package line.bot.pojo;

public enum ViewFormat {

	JSON("json");

	private final String text;

	private ViewFormat(final String text) {
		this.text = text;
	}

	@Override
	public String toString() {
		return text;
	}
}
