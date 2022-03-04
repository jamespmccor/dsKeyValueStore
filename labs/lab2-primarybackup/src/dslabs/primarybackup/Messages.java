package dslabs.primarybackup;

import dslabs.framework.Message;
import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Application;
import lombok.Data;

/* -------------------------------------------------------------------------
    ViewServer Messages
   -----------------------------------------------------------------------*/
@Data
class Ping implements Message {
    private final int viewNum;
}

@Data
class GetView implements Message {
}

@Data
class ViewReply implements Message {
    private final View view;
}

@Data
class ViewError implements Message {
}

/* -------------------------------------------------------------------------
    Primary-Backup Messages
   -----------------------------------------------------------------------*/
@Data
class Request implements Message {
    private final dslabs.atmostonce.Request amoCommand;
}

@Data
class Reply implements Message {
    private final AMOResult amoResult;
}

@Data
class StateTransfer implements Message {
    private final View view;
    private final AMOApplication<Application> app;
}

@Data
class StateTransferAck implements Message {
    private final int viewNum;
}


@Data
class Forward extends Request {
    private final int seqNum;

    public Forward(int seqNum, dslabs.atmostonce.Request cmd){
        super(cmd);
        this.seqNum = seqNum;
    }
}

@Data
class ForwardAck implements Message {
    private final int seqNum;
}
