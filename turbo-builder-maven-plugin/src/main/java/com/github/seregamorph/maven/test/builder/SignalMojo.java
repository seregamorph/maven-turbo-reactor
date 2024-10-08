package com.github.seregamorph.maven.test.builder;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import java.util.function.Consumer;

@Mojo(
    name = "signal",
    defaultPhase = LifecyclePhase.PACKAGE,
    threadSafe = true)
public class SignalMojo extends AbstractMojo {

    /**
     * See {@link com.github.seregamorph.maven.test.builder.SignalingExecutorCompletionService#ATTR_SIGNALER}
     */
    private static final String ATTR_SIGNALER = "signaler";

    @Parameter(defaultValue = "${project}", readonly = true, required = true)
    private MavenProject project;

    @SuppressWarnings("unchecked")
    @Override
    public void execute() {
        // note: ClassLoader for this class is different in the turbo-builder-maven-plugin and turbo-builder extension
        Consumer<MavenProject> signaler = (Consumer<MavenProject>) project.getContextValue(ATTR_SIGNALER);
        if (signaler != null) {
            signaler.accept(project);
        }
    }
}
