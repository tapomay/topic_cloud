package cs185c.project;

public class StatDataPoint {
	private String stockSymbol;
	private String lastTimestamp;
	private Double meanHigh;
	private Double meanLow;
	private Double meanOpen;
	private Double meanClose;
	private Double meanVolume;
	private Double lastClose;

	private StatDataPoint() {
		super();
	}

	public StatDataPoint(String stockSymbol, String lastTimestamp, Double meanHigh, Double meanLow, Double meanOpen,
			Double meanClose, Double meanVolume, Double lastClose) {
		super();
		this.stockSymbol = stockSymbol;
		this.lastTimestamp = lastTimestamp;
		this.meanHigh = meanHigh;
		this.meanLow = meanLow;
		this.meanOpen = meanOpen;
		this.meanClose = meanClose;
		this.meanVolume = meanVolume;
		this.lastClose = lastClose;
	}

	public String getStockSymbol() {
		return stockSymbol;
	}

	public void setStockSymbol(String stockSymbol) {
		this.stockSymbol = stockSymbol;
	}

	public String getLastTimestamp() {
		return lastTimestamp;
	}

	public void setLastTimestamp(String lastTimestamp) {
		this.lastTimestamp = lastTimestamp;
	}

	public Double getMeanHigh() {
		return meanHigh;
	}

	public void setMeanHigh(Double meanHigh) {
		this.meanHigh = meanHigh;
	}

	public Double getMeanLow() {
		return meanLow;
	}

	public void setMeanLow(Double meanLow) {
		this.meanLow = meanLow;
	}

	public Double getMeanOpen() {
		return meanOpen;
	}

	public void setMeanOpen(Double meanOpen) {
		this.meanOpen = meanOpen;
	}

	public Double getMeanClose() {
		return meanClose;
	}

	public void setMeanClose(Double meanClose) {
		this.meanClose = meanClose;
	}

	public Double getMeanVolume() {
		return meanVolume;
	}

	public void setMeanVolume(Double meanVolume) {
		this.meanVolume = meanVolume;
	}

	public Double getLastClose() {
		return lastClose;
	}

	public void setLastClose(Double lastClose) {
		this.lastClose = lastClose;
	}

	@Override
	public String toString() {
		return "StatDataPoint [stockSymbol=" + stockSymbol + ", lastTimestamp=" + lastTimestamp + ", meanHigh="
				+ meanHigh + ", meanLow=" + meanLow + ", meanOpen=" + meanOpen + ", meanClose=" + meanClose
				+ ", meanVolume=" + meanVolume + ", lastClose=" + lastClose + "]";
	}

}
