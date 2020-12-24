package tech.mlsql.test.binlogserver;


public class TestStringFormat {
    public static void main(String[] args) {
        System.out.println("%013d".format("567"));
        System.out.println(String.format("%013d", 567));
        System.out.println(String.class.getName());
        System.out.println(String.class.getSimpleName());
    }
}
