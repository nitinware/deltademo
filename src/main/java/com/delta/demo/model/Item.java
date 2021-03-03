package com.delta.demo.model;

public class Item {

    private Long itemId;
    private String  descLong;
    private String descShort;
    //private Integer store;

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public String getDescLong() {
        return descLong;
    }

    public void setDescLong(String descLong) {
        this.descLong = descLong;
    }

    public String getDescShort() {
        return descShort;
    }

    public void setDescShort(String descShort) {
        this.descShort = descShort;
    }

    /*public Integer getStore() {
        return store;
    }

    public void setStore(Integer store) {
        this.store = store;
    }*/
}
