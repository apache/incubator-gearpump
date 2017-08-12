package org.apache.gearpump.sql.table;

import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;

public class SampleString {

    public static JavaStream<String> WORDS;

    public static class Stream {
        public final Message[] KV = {new Message("001", "This is a good start, bingo!! bingo!!")};
    }

    public static class Message {
        public final String ID;
        public final String WORD;

        public Message(String ID, String WORD) {
            this.ID = ID;
            this.WORD = WORD;
        }
    }


}
