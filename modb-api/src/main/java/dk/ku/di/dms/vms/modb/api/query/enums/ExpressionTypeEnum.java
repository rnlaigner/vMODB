package dk.ku.di.dms.vms.modb.api.query.enums;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum ExpressionTypeEnum {

    // value expression

    LESS_THAN("<"),

    GREATER_THAN(">"),

    LESS_THAN_OR_EQUAL("<="),

    GREATER_THAN_OR_EQUAL(">="),

    EQUALS("="),

    NOT_EQUALS("<>"),

    // only for string or char
    LIKE("like"),

    // nullable expression

    IS_NULL("is null"),

    IS_NOT_NULL("is not null"),

    // boolean expression

    OR("or"),

    AND("and"),

    // set expression

    IN("in"),

    NOT_IN("not in"),

    NOT("not"),

    EXISTS("exists");

    public final String value;

    ExpressionTypeEnum(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static boolean equality(ExpressionTypeEnum expressionType){
        return expressionType == EQUALS || expressionType == IN;
    }

    private static final Map<String, ExpressionTypeEnum> LOOKUP =
            Arrays.stream(values())
                    .collect(Collectors.toMap(ExpressionTypeEnum::value, c -> c));

    public static ExpressionTypeEnum fromValue(String value) {
        ExpressionTypeEnum cmd = LOOKUP.get(value.toLowerCase());
        if (cmd == null) {
            throw new IllegalArgumentException("Unknown command: " + value);
        }
        return cmd;
    }

}
