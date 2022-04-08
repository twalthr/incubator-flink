package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.SpecializedFunction.ExpressionEvaluator;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.util.Preconditions;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/** Default runtime implementation for {@link ExpressionEvaluator} */
@Internal
public class DefaultExpressionEvaluator implements ExpressionEvaluator {
    private static final long serialVersionUID = 1L;

    private final GeneratedFunction<RichFunction> generatedClass;
    private final MethodType methodType;
    private transient RichFunction instance;

    public DefaultExpressionEvaluator(
            GeneratedFunction<RichFunction> generatedClass,
            Class<?> returnClass,
            Class<?>[] argClasses) {
        this.generatedClass = generatedClass;
        this.methodType = MethodType.methodType(returnClass, argClasses);
    }

    @Override
    public MethodHandle openHandleInternal(RuntimeContext context) {
        Preconditions.checkState(
                instance == null,
                "Expression evaluator for '%s' has already been opened.",
                instance);
        try {
            instance = generatedClass.newInstance(context.getUserCodeClassLoader());
            instance.setRuntimeContext(context);
            instance.open(new Configuration());
            final MethodHandles.Lookup publicLookup = MethodHandles.publicLookup();
            return publicLookup
                    .findVirtual(instance.getClass(), "eval", methodType)
                    .bindTo(instance);
        } catch (Exception e) {
            throw new TableException(
                    String.format("Expression evaluator for '%s' could not be opened.", instance),
                    e);
        }
    }

    @Override
    public void close() {
        try {
            instance.close();
        } catch (Exception e) {
            throw new TableException(
                    String.format("Expression evaluator for '%s' could not be closed.", instance),
                    e);
        }
    }
}
