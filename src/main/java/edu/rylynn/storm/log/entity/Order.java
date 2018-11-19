package edu.rylynn.storm.log.entity;

import java.util.Date;

//orderNumber: XX | orderDate: XX | paymentNumber: XX | paymentDate: XX | merchantName: XX | sku: [ skuName: XX skuNum: XX skuCode: XX skuPrice: XX totalSkuPrice: XX;skuName: XX skuNum: XX skuCode: XX skuPrice: XX totalSkuPrice: XX;] | price: [ totalPrice: XX discount: XX paymentPrice: XX ]
public class Order {
    private long orderNumber;
    private Date orderDate;
    private long paymentNumber;
    private Date paymentDate;
    private String merchantName;
    private Sku[] sku;
    private double totalPrice;
    private double discount;
    private double paymenPrice;

    public Order(long orderNumber, Date orderDate, long paymentNumber, Date paymentDate, String merchantName, Sku[] sku, double totlaPrice, double discount, double paymenPrice) {
        this.orderNumber = orderNumber;
        this.orderDate = orderDate;
        this.paymentNumber = paymentNumber;
        this.paymentDate = paymentDate;
        this.merchantName = merchantName;
        this.sku = sku;
        this.totalPrice = totlaPrice;
        this.discount = discount;
        this.paymenPrice = paymenPrice;
    }


    @Override
    public String toString() {
        return "orderNumber: XX | orderDate: XX | paymentNumber: XX | paymentDate: XX | merchantName: XX | sku: [ skuName: XX skuNum: XX skuCode: XX skuPrice: XX totalSkuPrice: XX;skuName: XX skuNum: XX skuCode: XX skuPrice: XX totalSkuPrice: XX;] | price: [ totalPrice: XX discount: XX paymentPrice: XX ]";
    }

    public Sku[] getSku() {
        return sku;
    }

    public void setSku(Sku[] sku) {
        this.sku = sku;
    }

    public long getOrderNumber() {
        return orderNumber;
    }

    public void setOrderNumber(long orderNumber) {
        this.orderNumber = orderNumber;
    }

    public Date getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(Date orderDate) {
        this.orderDate = orderDate;
    }

    public long getPaymentNumber() {
        return paymentNumber;
    }

    public void setPaymentNumber(long paymentNumber) {
        this.paymentNumber = paymentNumber;
    }

    public Date getPaymentDate() {
        return paymentDate;
    }

    public void setPaymentDate(Date paymentDate) {
        this.paymentDate = paymentDate;
    }

    public double getPaymenPrice() {
        return paymenPrice;
    }

    public void setPaymenPrice(double paymenPrice) {
        this.paymenPrice = paymenPrice;
    }

    public String getMerchantName() {
        return merchantName;
    }

    public void setMerchantName(String merchantName) {
        this.merchantName = merchantName;
    }

    public double getTotalPrice() {
        return totalPrice;
    }

    public void setTotalPrice(double totalPrice) {
        this.totalPrice = totalPrice;
    }

    public double getDiscount() {
        return discount;
    }

    public void setDiscount(double discount) {
        this.discount = discount;
    }

    private class Sku {
        private long skuName;
        private int skuNum;
        private String skuCode;
        private double skuPrice;
        private double totalSkuPrice;
    }

}
