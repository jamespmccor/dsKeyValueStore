package dslabs.clientserver;

import dslabs.atmostonce.AMOResult;
import dslabs.framework.Message;
import lombok.Data;

@Data
class Request implements Message {
    private final dslabs.atmostonce.Request command;
}

@Data
class Reply implements Message {
    private final AMOResult result;
}