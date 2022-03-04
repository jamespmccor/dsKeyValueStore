package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application>
        implements Application {
    @Getter @NonNull private final T application;

    // Your code here...
    private final Map<Address, AMOResult> clientMap = new HashMap<>();

    @Override
    public AMOResult execute(Command command) {
        if (!(command instanceof Request)) {
            throw new IllegalArgumentException();
        }

        Request amoCommand = (Request) command;

        if (alreadyExecuted(amoCommand)) {
            if (amoCommand.num() < clientMap.get(amoCommand.sender()).num()) {
                return null;
            } else {
                return clientMap.get(amoCommand.sender());
            }
        }

        AMOResult res = new AMOResult(amoCommand.num(), application.execute(amoCommand.command()));
        clientMap.put(amoCommand.sender(), res);
        return res;
    }

    public Result executeReadOnly(Command command) {
        if (!command.readOnly()) {
            throw new IllegalArgumentException();
        }

        if (command instanceof Request) {
            return execute(command);
        }

        return application.execute(command);
    }

    public boolean alreadyExecuted(Request amoCommand) {
        return clientMap.containsKey(amoCommand.sender()) &&
                clientMap.get(amoCommand.sender()).num() >= amoCommand.num();
    }
}
