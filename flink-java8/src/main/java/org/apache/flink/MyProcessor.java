package org.apache.flink;

import com.sun.source.util.JavacTask;
import com.sun.source.util.Trees;
import com.sun.tools.javac.api.ClientCodeWrapper;
import com.sun.tools.javac.code.Symbol;
import com.sun.tools.javac.code.Type;
import com.sun.tools.javac.processing.JavacProcessingEnvironment;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.JCTree.JCExpression;
import com.sun.tools.javac.tree.JCTree.JCMethodInvocation;
import com.sun.tools.javac.tree.JCTree.JCNewClass;
import com.sun.tools.javac.tree.TreeMaker;
import com.sun.tools.javac.tree.TreeTranslator;
import com.sun.tools.javac.util.JCDiagnostic;
import com.sun.tools.javac.util.Names;

import javax.annotation.processing.Completion;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticListener;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.sun.tools.javac.code.Type.ClassType;
import static com.sun.tools.javac.code.Type.MethodType;

public class MyProcessor implements Processor {

	public static final Map<String, String> proxies = new HashMap<>();
	static {
		proxies.put("org.apache.flink.api.common.functions.MapFunction",
			"org.apache.flink.ProxyMapFunction");
	}

	private Trees trees;
	private TreeMaker treeMaker;
	private Names names;

	@Override
	public Set<String> getSupportedOptions() {
		return Collections.emptySet();
	}

	@Override
	public Set<String> getSupportedAnnotationTypes() {
		return Collections.singleton("*");
	}

	@Override
	public SourceVersion getSupportedSourceVersion() {
		return SourceVersion.latestSupported();
	}

	@Override
	public void init(ProcessingEnvironment processingEnv) {
		this.trees = Trees.instance(processingEnv);
		this.treeMaker = TreeMaker.instance(((JavacProcessingEnvironment) processingEnv).getContext());
		this.names = Names.instance(((JavacProcessingEnvironment) processingEnv).getContext());
	}

	@Override
	public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
		for (Element element : roundEnv.getRootElements()) {
			try {
				transform(element);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return false;
	}

	@Override
	public Iterable<? extends Completion> getCompletions(Element element, AnnotationMirror annotation, ExecutableElement member, String userText) {
		return Collections.emptyList();
	}

	// --------------------------------------------------------------------------------------------

	private void transform(Element e) throws Exception {
		if (!(e instanceof Symbol.ClassSymbol)) {
			return;
		}
		final Symbol.ClassSymbol clazz = (Symbol.ClassSymbol) e;

		final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		final InferredTypesListener listener =  new InferredTypesListener();
		// skip annotations and print information about inferred types
		final JavacTask task = (JavacTask) compiler.getTask(null, null, listener,
			Arrays.asList("-proc:none", "-XDverboseResolution=deferred-inference"), null,
			Collections.singleton(clazz.sourcefile));
		// analyze without creating files
		task.analyze();

		JCTree tree = (JCTree) trees.getTree(e);
  		tree.accept(new ProxyTranslator(listener.replacements));
		System.out.println(tree);
	}

	// --------------------------------------------------------------------------------------------

	private static class InferredTypesListener implements DiagnosticListener<JavaFileObject> {

		private final Map<Long, ProxyReplacement> replacements = new HashMap<>();

		@Override
		public void report(Diagnostic<? extends JavaFileObject> diagnostic) {
			// check for type inference diagnostic
			if (diagnostic.getCode().equals("compiler.note.deferred.method.inst") &&
					diagnostic instanceof ClientCodeWrapper.DiagnosticSourceUnwrapper) {
				final JCDiagnostic jcd = ((ClientCodeWrapper.DiagnosticSourceUnwrapper) diagnostic).d;
				// check for method argument type inference
				if (jcd.getArgs().length > 0 && jcd.getArgs()[1] instanceof MethodType) {
					final MethodType method = (MethodType) jcd.getArgs()[1];
					for (Type t : method.argtypes) {
						// check for interesting types
						if (t instanceof ClassType) {
							final String className = ((ClassType) t).tsym.getQualifiedName().toString();
							final com.sun.tools.javac.util.List<Type> types = ((ClassType) t).typarams_field;
							final String proxy = proxies.get(className);
							if (proxy != null) {
								final ProxyReplacement r = new ProxyReplacement(proxy, types);
								replacements.put(jcd.getPosition(), r);
							}
						}
					}
				}
			}
		}
	}

	private static class ProxyReplacement {
		public final String proxy;
		public final com.sun.tools.javac.util.List<Type> types;

		public ProxyReplacement(String proxy, com.sun.tools.javac.util.List<Type> types) {
			this.proxy = proxy;
			this.types = types;
		}
	}

	// --------------------------------------------------------------------------------------------

	private class ProxyTranslator extends TreeTranslator {

		private final Map<Long, ProxyReplacement> replacements;

		public ProxyTranslator(Map<Long, ProxyReplacement> replacements) {
			this.replacements = replacements;
		}

		@Override
		public void visitApply(JCMethodInvocation invocation) {
			super.visitApply(invocation);
			final Long pos = (long) invocation.getPreferredPosition();
			final ProxyReplacement replacement = replacements.get(pos);
			if (replacement != null) {
				// new org.apache.flink.ProxyMapFunction<String, String>(map){ }
				final JCNewClass wrapper = treeMaker.NewClass(
					null,
					null,
					// org.apache.flink.ProxyMapFunction<String, String>
					treeMaker.TypeApply(
						// org.apache.flink.ProxyMapFunction
						qualifiedClass(replacement.proxy),
						// <String, String>
						codeToTree(replacement.types)
					),
					com.sun.tools.javac.util.List.of(invocation.args.head),
					treeMaker.ClassDef(
						treeMaker.Modifiers(0),
						names.fromString(""),
						com.sun.tools.javac.util.List.nil(),
						null,
						com.sun.tools.javac.util.List.nil(),
						com.sun.tools.javac.util.List.nil())
				);
				result = treeMaker.Apply(invocation.typeargs, invocation.meth, com.sun.tools.javac.util.List.of(wrapper));
			}
		}

		private JCExpression qualifiedClass(String path) {
			final String[] splitPath = path.split("\\.");
			JCExpression expr = treeMaker.Ident(names.fromString(splitPath[0]));
			for (int i = 1; i < splitPath.length; i++) {
				expr = treeMaker.Select(expr, names.fromString(splitPath[i]));
			}
			return expr;
		}

		private com.sun.tools.javac.util.List<JCExpression> codeToTree(com.sun.tools.javac.util.List<Type> types) {
			final List<JCExpression> converted = new ArrayList<>(types.length());
			for (Type t : types) {
				converted.add(codeToTree(t));
			}
			return com.sun.tools.javac.util.List.from(converted);
		}

		private JCExpression codeToTree(Type t) {
			switch (t.getTag()) {
				case TYPEVAR:
					return treeMaker.Ident(t.tsym);
				case WILDCARD:
					Type.WildcardType wildcard = (Type.WildcardType) t;
					return treeMaker.Wildcard(treeMaker.TypeBoundKind(wildcard.kind), codeToTree(wildcard.type));
				case CLASS:
					if (t.getTypeArguments().isEmpty()) {
						return qualifiedClass(t.toString());
					} else {
						final com.sun.tools.javac.util.List<JCExpression> params = codeToTree(t.getTypeArguments());
						return treeMaker.TypeApply(codeToTree(t.tsym.erasure_field), params);
					}
				case ARRAY:
					final Type.ArrayType arrayType = (Type.ArrayType) t;
					return treeMaker.TypeArray(codeToTree(arrayType.elemtype));
				default:
					throw new UnsupportedOperationException("Unexpected type." + t.getTag());
			}
		}
	}
}
