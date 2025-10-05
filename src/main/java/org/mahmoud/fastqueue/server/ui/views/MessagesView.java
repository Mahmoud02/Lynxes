package org.mahmoud.fastqueue.server.ui.views;

import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.combobox.ComboBox;
import com.vaadin.flow.component.formlayout.FormLayout;
import com.vaadin.flow.component.html.H1;
import com.vaadin.flow.component.html.H2;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.textfield.IntegerField;
import com.vaadin.flow.component.textfield.TextArea;
import com.vaadin.flow.component.textfield.TextField;
import com.vaadin.flow.router.PageTitle;
import com.vaadin.flow.router.Route;
import com.vaadin.flow.theme.lumo.LumoUtility;
import org.mahmoud.fastqueue.server.ui.layout.MainLayout;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Messages view for publishing and consuming messages
 */
@Route(value = "messages", layout = MainLayout.class)
@PageTitle("Messages | FastQueue2")
public class MessagesView extends VerticalLayout {
    
    private ComboBox<String> topicComboBox;
    private TextArea messageTextArea;
    private TextField offsetField;
    private VerticalLayout messagesDisplay;
    
    public MessagesView() {
        addClassNames(LumoUtility.Padding.LARGE);
        
        H1 title = new H1("Message Operations");
        title.addClassNames(LumoUtility.FontSize.XXXLARGE, LumoUtility.Margin.Bottom.LARGE);
        
        // Initialize topic list
        List<String> topics = List.of("orders", "notifications", "events", "logs", "metrics");
        
        // Publish Section
        VerticalLayout publishSection = createPublishSection(topics);
        
        // Consume Section
        VerticalLayout consumeSection = createConsumeSection(topics);
        
        add(title, publishSection, consumeSection);
    }
    
    private VerticalLayout createPublishSection(List<String> topics) {
        VerticalLayout section = new VerticalLayout();
        section.addClassNames(LumoUtility.Border.ALL, LumoUtility.BorderRadius.MEDIUM, 
                             LumoUtility.Padding.MEDIUM, LumoUtility.Background.BASE);
        
        H2 sectionTitle = new H2("Publish Message");
        sectionTitle.addClassNames(LumoUtility.Margin.Top.NONE, LumoUtility.Margin.Bottom.MEDIUM);
        
        FormLayout formLayout = new FormLayout();
        
        topicComboBox = new ComboBox<>("Select Topic");
        topicComboBox.setItems(topics);
        topicComboBox.setPlaceholder("Choose a topic");
        topicComboBox.setRequired(true);
        
        messageTextArea = new TextArea("Message Content");
        messageTextArea.setPlaceholder("Enter your message here...");
        messageTextArea.setRequired(true);
        
        formLayout.add(topicComboBox, messageTextArea);
        formLayout.setResponsiveSteps(new FormLayout.ResponsiveStep("0", 1));
        
        Button publishButton = new Button("Publish Message", VaadinIcon.UPLOAD.create());
        publishButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
        publishButton.addClickListener(e -> publishMessage());
        
        HorizontalLayout buttonLayout = new HorizontalLayout(publishButton);
        buttonLayout.setJustifyContentMode(JustifyContentMode.END);
        
        section.add(sectionTitle, formLayout, buttonLayout);
        return section;
    }
    
    private VerticalLayout createConsumeSection(List<String> topics) {
        VerticalLayout section = new VerticalLayout();
        section.addClassNames(LumoUtility.Border.ALL, LumoUtility.BorderRadius.MEDIUM, 
                             LumoUtility.Padding.MEDIUM, LumoUtility.Background.BASE);
        
        H2 sectionTitle = new H2("Consume Messages");
        sectionTitle.addClassNames(LumoUtility.Margin.Top.NONE, LumoUtility.Margin.Bottom.MEDIUM);
        
        FormLayout formLayout = new FormLayout();
        
        ComboBox<String> consumeTopicComboBox = new ComboBox<>("Select Topic");
        consumeTopicComboBox.setItems(topics);
        consumeTopicComboBox.setPlaceholder("Choose a topic");
        consumeTopicComboBox.setRequired(true);
        
        offsetField = new TextField("Offset");
        offsetField.setPlaceholder("Message offset (optional)");
        
        formLayout.add(consumeTopicComboBox, offsetField);
        formLayout.setResponsiveSteps(new FormLayout.ResponsiveStep("0", 1));
        
        Button consumeButton = new Button("Consume Message", VaadinIcon.DOWNLOAD.create());
        consumeButton.addThemeVariants(ButtonVariant.LUMO_CONTRAST);
        consumeButton.addClickListener(e -> {
            String offsetValue = offsetField.getValue();
            Integer offset = null;
            if (offsetValue != null && !offsetValue.trim().isEmpty()) {
                try {
                    offset = Integer.parseInt(offsetValue);
                } catch (NumberFormatException ex) {
                    // Invalid offset, use null
                }
            }
            consumeMessage(consumeTopicComboBox.getValue(), offset);
        });
        
        HorizontalLayout buttonLayout = new HorizontalLayout(consumeButton);
        buttonLayout.setJustifyContentMode(JustifyContentMode.END);
        
        // Messages display area
        messagesDisplay = new VerticalLayout();
        messagesDisplay.addClassNames(LumoUtility.Border.ALL, LumoUtility.BorderRadius.SMALL, 
                                    LumoUtility.Padding.SMALL, LumoUtility.Background.CONTRAST_5);
        messagesDisplay.setMaxHeight("300px");
        messagesDisplay.setWidthFull();
        
        section.add(sectionTitle, formLayout, buttonLayout, messagesDisplay);
        return section;
    }
    
    private void publishMessage() {
        String topic = topicComboBox.getValue();
        String message = messageTextArea.getValue();
        
        if (topic == null || message.trim().isEmpty()) {
            return;
        }
        
        // Mock publish operation
        Span successMessage = new Span("✓ Message published to topic '" + topic + "' at " + LocalDateTime.now());
        successMessage.addClassNames(LumoUtility.TextColor.SUCCESS, LumoUtility.FontSize.SMALL);
        
        // Clear form
        messageTextArea.clear();
        
        // Show success feedback
        add(successMessage);
    }
    
    private void consumeMessage(String topic, Integer offset) {
        if (topic == null) {
            return;
        }
        
        // Mock consume operation
        String messageContent = "Sample message from topic '" + topic + "'";
        if (offset != null) {
            messageContent += " at offset " + offset;
        }
        
        VerticalLayout messageCard = new VerticalLayout();
        messageCard.addClassNames(LumoUtility.Border.ALL, LumoUtility.BorderRadius.SMALL, 
                                LumoUtility.Padding.SMALL, LumoUtility.Background.BASE);
        
        Span topicSpan = new Span("Topic: " + topic);
        topicSpan.addClassNames(LumoUtility.FontWeight.BOLD, LumoUtility.TextColor.PRIMARY);
        
        Span contentSpan = new Span(messageContent);
        contentSpan.addClassNames(LumoUtility.FontSize.SMALL);
        
        Span timestampSpan = new Span("Consumed at: " + LocalDateTime.now());
        timestampSpan.addClassNames(LumoUtility.TextColor.SECONDARY, LumoUtility.FontSize.XSMALL);
        
        messageCard.add(topicSpan, contentSpan, timestampSpan);
        messagesDisplay.add(messageCard);
        
        // Clear offset field
        offsetField.clear();
    }
}
