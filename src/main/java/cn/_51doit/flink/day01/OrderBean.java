package cn._51doit.flink.day01;

public class OrderBean {

    private String province;

    private String city;

    private Double money;

    public OrderBean() {}

    public OrderBean(String province, String city, Double money) {
        this.province = province;
        this.city = city;
        this.money = money;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public Double getMoney() {
        return money;
    }

    public void setMoney(Double money) {
        this.money = money;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", money=" + money +
                '}';
    }
}
