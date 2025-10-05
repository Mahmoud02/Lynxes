package org.mahmoud.fastqueue.server.ui.components;

import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.icon.Icon;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.theme.lumo.LumoUtility;

/**
 * Reusable status indicator component for showing health/status information
 * Used across different views for consistent status display
 */
public class StatusIndicator extends HorizontalLayout {
    
    public enum Status {
        HEALTHY(VaadinIcon.CHECK_CIRCLE, LumoUtility.TextColor.SUCCESS),
        WARNING(VaadinIcon.WARNING, LumoUtility.TextColor.WARNING),
        ERROR(VaadinIcon.CLOSE_CIRCLE, LumoUtility.TextColor.ERROR),
        UNKNOWN(VaadinIcon.QUESTION_CIRCLE, LumoUtility.TextColor.SECONDARY);
        
        private final VaadinIcon icon;
        private final String colorClass;
        
        Status(VaadinIcon icon, String colorClass) {
            this.icon = icon;
            this.colorClass = colorClass;
        }
        
        public VaadinIcon getIcon() { return icon; }
        public String getColorClass() { return colorClass; }
    }
    
    public StatusIndicator(Status status, String text) {
        setAlignItems(Alignment.CENTER);
        
        Icon statusIcon = status.getIcon().create();
        statusIcon.addClassNames(status.getColorClass(), LumoUtility.FontSize.SMALL);
        
        Span statusText = new Span(text);
        statusText.addClassNames(status.getColorClass(), LumoUtility.FontSize.SMALL);
        
        add(statusIcon, statusText);
    }
}
