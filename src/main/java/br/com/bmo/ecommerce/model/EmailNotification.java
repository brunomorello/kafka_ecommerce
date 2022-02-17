package br.com.bmo.ecommerce.model;

public class EmailNotification {
    private final String header, body;

    public EmailNotification(String header, String body) {
        this.header = header;
        this.body = body;
    }
}
