package org.mahmoud.fastqueue.server.ui.components;

import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.icon.Icon;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.theme.lumo.LumoUtility;

/**
 * Reusable metric card component for displaying key-value metrics
 * Used across different views for consistent metric display
 */
public class MetricCard extends VerticalLayout {
    
    public MetricCard(String title, String value, VaadinIcon icon, String description) {
        addClassNames(LumoUtility.Border.ALL, LumoUtility.BorderRadius.SMALL, 
                     LumoUtility.Padding.SMALL, LumoUtility.Background.CONTRAST_5);
        setAlignItems(Alignment.CENTER);
        
        Icon cardIcon = icon.create();
        cardIcon.addClassNames(LumoUtility.TextColor.PRIMARY, LumoUtility.FontSize.LARGE);
        
        Span valueSpan = new Span(value);
        valueSpan.addClassNames(LumoUtility.FontSize.LARGE, LumoUtility.FontWeight.BOLD, 
                               LumoUtility.TextColor.PRIMARY);
        
        Span titleSpan = new Span(title);
        titleSpan.addClassNames(LumoUtility.FontWeight.BOLD);
        
        Span descSpan = new Span(description);
        descSpan.addClassNames(LumoUtility.TextColor.SECONDARY, LumoUtility.FontSize.SMALL);
        
        add(cardIcon, valueSpan, titleSpan, descSpan);
    }
}
