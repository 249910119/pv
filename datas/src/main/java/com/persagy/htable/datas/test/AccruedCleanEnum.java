package com.persagy.htable.datas.test;

public enum AccruedCleanEnum {


    SPREAD("1","发票"),OTHER("0","收入"),NON_BUSINESS("2","支出");

    private String index;
    private String name;


    AccruedCleanEnum(String index, String name) {
        this.index = index;
        this.name = name;
    }


    public static String getName(String index){

        for(AccruedCleanEnum accruedCleanEnum : AccruedCleanEnum.values()){
            if(accruedCleanEnum.getIndex().equals(index)){
                return accruedCleanEnum.name;
            }
        }
        return "";
    }


    public String getIndex() {
        return index;
    }


    public String getName() {
        return name;
    }

}
