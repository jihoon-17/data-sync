package cn.com.xx.reader.enums;

/**
 * 目标库类型
 */
public enum SchemaType {

    STARROCKS("starrocks"),MYSQL("mysql"),HANA("hana");

    private String value;

    SchemaType(String value) {

        this.value = value;

    }

    public String getValue() {
        return value;
    }


    public static void main(String[] args) {
        System.out.println(SchemaType.STARROCKS.getValue());
    }
}
