package cs185c.project;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PriceDataPoint {
	private String timestamp;
	private String stockSymbol;
	private Float open;
	private Float high;
	private Float low;
	private Float close;
	private Long volume;

	private PriceDataPoint() {
		super();
	}

	private static final String SAMPLE_LINE = "2015-04-01,124.82,125.120003,123.099998,124.25,40621400,119.463174";
	private static final String[] SAMPLE_SPLIT;
	private static final String DATE_FORMAT = "yyyy-M-dd";
	private static final SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);

	static {
		SAMPLE_SPLIT = SAMPLE_LINE.split(",");
	}

	public static PriceDataPoint fromLine(String line, String stockSymbol) {
		PriceDataPoint ret = null;
		String[] split = line.split(",");
		if (split.length == SAMPLE_SPLIT.length) {
			try {
				Date dateObj = sdf.parse(split[0]); // check value by parsing
				String date = sdf.format(dateObj);
				Float open = Float.parseFloat(split[1]);
				Float high = Float.parseFloat(split[2]);
				Float low = Float.parseFloat(split[3]);
				Float close = Float.parseFloat(split[4]);
				Long volume = Long.parseLong(split[5]);
				Float adjClose = Float.parseFloat(split[6]);

				ret = new PriceDataPoint(date, stockSymbol, open, high, low, close, volume);
			} catch (NumberFormatException | ParseException nfe) {
				System.out.println("Failed to parse line: " + nfe);
			}
		}
		return ret;
	}

	public PriceDataPoint(String timestamp, String stockSymbol, Float open, Float high, Float low, Float close,
			Long volume) {
		super();
		this.timestamp = timestamp;
		this.stockSymbol = stockSymbol;
		this.open = open;
		this.high = high;
		this.low = low;
		this.close = close;
		this.volume = volume;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public Float getOpen() {
		return open;
	}

	public void setOpen(Float open) {
		this.open = open;
	}

	public Float getHigh() {
		return high;
	}

	public void setHigh(Float high) {
		this.high = high;
	}

	public Float getLow() {
		return low;
	}

	public void setLow(Float low) {
		this.low = low;
	}

	public Float getClose() {
		return close;
	}

	public void setClose(Float close) {
		this.close = close;
	}

	public Long getVolume() {
		return volume;
	}

	public void setVolume(Long volume) {
		this.volume = volume;
	}

	public String getStockSymbol() {
		return stockSymbol;
	}

	public void setStockSymbol(String stockSymbol) {
		this.stockSymbol = stockSymbol;
	}

	@Override
	public String toString() {
		return "PriceDataPoint [timestamp=" + timestamp + ", stockSymbol=" + stockSymbol + ", open=" + open + ", high="
				+ high + ", low=" + low + ", close=" + close + ", volume=" + volume + "]";
	}

}
