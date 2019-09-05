package com.ljl.hadoop.common.key;

import com.sun.istack.NotNull;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderKey implements WritableComparable<OrderKey> {

    private String orderId;
    private float price;
    private String prodId;

    public OrderKey() {
        super();
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public String getProdId() {
        return prodId;
    }

    public void setProdId(String prodId) {
        this.prodId = prodId;
    }

    @Override
    public int compareTo(@NotNull OrderKey o) {
        if (o.orderId.equals(this.orderId)) {
            return Float.compare(o.price, this.price);
        } else {
            return this.orderId.compareTo(o.orderId);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeFloat(price);
        out.writeUTF(prodId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.price = in.readFloat();
        this.prodId = in.readUTF();
    }

    @Override
    public String toString() {
        return orderId + "\t" + prodId + "\t" + price;
    }
}
