package io.muayed.finalproject;

public class Performance {
    private String timestamp;
    private Double totalMem;
    private Double currentMem;

    public Performance() {
    }

    public Performance(String timestamp, Double totalMem, Double currentMem) {
        this.timestamp = timestamp;
        this.totalMem = totalMem;
        this.currentMem = currentMem;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTotalMem() {
        return totalMem;
    }

    public void setTotalMem(Double totalMem) {
        this.totalMem = totalMem;
    }

    public Double getCurrentMem() {
        return currentMem;
    }

    public void setCurrentMem(Double currentMem) {
        this.currentMem = currentMem;
    }

//    public Double getMemUsagePercentage() {
//        return memUsagePercentage;
//    }


    @Override
    public String toString() {
        return "Performance{" +
                "timestamp='" + timestamp + '\'' +
                ", totalMem=" + totalMem +
                ", currentMem=" + currentMem +
                '}';
    }
}
