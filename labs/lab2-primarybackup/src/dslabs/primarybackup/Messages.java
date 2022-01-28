package dslabs.primarybackup;

import dslabs.framework.Message;
import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
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
    private final AMOCommand amoCommand;
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
}


@Data
class Forward extends Request {
    public Forward(AMOCommand cmd){
        super(cmd);
    }
}

@Data
class ForwardAck extends Request {
    public ForwardAck(AMOCommand cmd){
        super(cmd);
    }
}
