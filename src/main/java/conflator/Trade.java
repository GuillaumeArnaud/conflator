package conflator;

public class Trade implements Message<Trade> {
    private final String ticker;
    private final long initialQuantity;
    private long currentQuantity;
    private int numberOfMerges;

    public Trade(String ticker, long quantity) {
        this.ticker = ticker;
        this.initialQuantity = quantity;
        this.currentQuantity = quantity;
    }

    public long getInitialQuantity() {
        return initialQuantity;
    }

    public long getCurrentQuantity() {
        return currentQuantity;
    }

    public String getTicker() {
        return ticker;
    }

    @Override
    public String key() {
        return this.ticker;
    }

    @Override
    public String body() {
        return String.valueOf(currentQuantity);
    }

    @Override
    public boolean isMerged() {
        return this.initialQuantity != this.currentQuantity;
    }

    @Override
    public int mergesCount() {
        return numberOfMerges;
    }

    @Override
    public boolean isValid() {
        return false;
    }

    @Override
    public boolean merge(Trade message) {
        if (this.equals(message)) {
            this.currentQuantity += message.currentQuantity;
            numberOfMerges++;
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return ticker.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof Trade) {
            Trade other = (Trade) obj;
            return this.ticker.equals(other.getTicker());
        }
        return false;
    }
}