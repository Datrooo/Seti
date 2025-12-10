package org.example.socks5;

import org.xbill.DNS.ARecord;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Header;
import org.xbill.DNS.Message;
import org.xbill.DNS.Name;
import org.xbill.DNS.Section;
import org.xbill.DNS.Type;

import java.net.InetAddress;

public class DnsClient {

    public byte[] buildQuery(String hostname, int txId) throws Exception {
        Name name = Name.fromString(hostname.endsWith(".") ? hostname : hostname + ".");
        org.xbill.DNS.Record question =
                org.xbill.DNS.Record.newRecord(name, Type.A, DClass.IN);
        Message query = Message.newQuery(question);
        Header header = query.getHeader();
        header.setID(txId);
        return query.toWire();
    }

    public InetAddress parseResponse(byte[] data, int expectedTxId) throws Exception {
        Message response = new Message(data);
        Header header = response.getHeader();
        int id = header.getID();
        if (id != expectedTxId) {
            throw new IllegalStateException("Unexpected DNS transaction id: " + id +
                    ", expected " + expectedTxId);
        }

        org.xbill.DNS.Record[] answers = response.getSectionArray(Section.ANSWER);
        for (org.xbill.DNS.Record r : answers) {
            if (r.getType() == Type.A) {
                ARecord ar = (ARecord) r;
                return ar.getAddress();
            }
        }
        return null;
    }
}
