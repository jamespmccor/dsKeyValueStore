package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Command;
import lombok.Data;

@Data
public final class Request implements Command {
    private final int num;
    private final Address sender;
    private final Command command;
}
