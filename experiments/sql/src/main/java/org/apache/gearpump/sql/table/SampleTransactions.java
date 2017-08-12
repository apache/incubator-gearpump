package org.apache.gearpump.sql.table;

public class SampleTransactions {

    public static class Transactions {

        public final Order[] ORDERS = {
                new Order("001", 3),
                new Order("002", 5),
                new Order("003", 8),
                new Order("004", 15),
        };

        public final Product[] PRODUCTS = {
                new Product("001", "Book"),
                new Product("002", "Pen"),
                new Product("003", "Pencil"),
                new Product("004", "Ruler"),
        };
    }

    public static class Order {
        public final String ID;
        public final int QUANTITY;

        public Order(String ID, int QUANTITY) {
            this.ID = ID;
            this.QUANTITY = QUANTITY;
        }
    }

    public static class Product {
        public final String ID;
        public final String NAME;

        public Product(String ID, String NAME) {
            this.ID = ID;
            this.NAME = NAME;
        }
    }

}
