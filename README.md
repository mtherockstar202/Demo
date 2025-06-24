package com.barcap.gcst.manualpayments.service.integration;

import com.barcap.gcst.manualpayments.entity.mpi.Mpi;
import com.barcap.gcst.manualpayments.model.dto.payment.*;
import com.barcap.gcst.manualpayments.repository.MpiRepository;
import com.barcap.gcst.manualpayments.service.PaymentService;

import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import org.springframework.boot.test.mock.mockito.MockBean;

import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
class PaymentServiceIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15")
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test");

    @DynamicPropertySource
    static void overrideProps(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    private PaymentService paymentService;

    @Autowired
    private MpiRepository mpiRepository;

    @MockBean
    private ProducerTemplate producerTemplate; // Kafka is mocked here

    private PaymentDTO testDto;

    @BeforeEach
    void setup() {
        // Save required MPI record before payment creation
        Mpi mpi = Mpi.builder()
                .mpild("MPI-INTG")
                .controlName("TestControl")
                .directorowner("TestDirector")
                .gmoService("TestService")
                .build();
        mpiRepository.save(mpi);

        // Prepare DTO for payment creation and verification
        testDto = PaymentDTO.builder()
                .paymentId("PAY-INTG")
                .mpild("MPI-INTG")
                .counterpartyID("CP-999")
                .valueDate(LocalDate.now().plusDays(1))
                .ssiID("SSI-888")
                .paymentAmount(10000L)
                .paymentValue(BigDecimal.valueOf(10000.00))
                .paymentCurrency("INR")
                .charges(Collections.emptySet())
                .debtor(new DebtorDTO("Debtor X", "ACC1234"))
                .debtorAgent(new AgentDTO("DEBTBIC"))
                .fromBic(new BicDTO("FROMBIC"))
                .toBic(new BicDTO("TOBIC"))
                .creditor(new CreditorDTO("Creditor Y", "ACC5678"))
                .creditorAgent(new AgentDTO("CREDBIC"))
                .build();
    }

    @Test
    void endToEnd_createAndVerifyPayment_success() {
        String correlationId = "flow-corr-id";

        // Step 1: Create payment
        PaymentDTO created = paymentService.createPayment(testDto, correlationId);
        assertNotNull(created);
        assertEquals("PAY-INTG", created.paymentId());
        assertEquals("INR", created.paymentCurrency());

        // Step 2: Verify payment
        PaymentDTO verified = paymentService.verifyPayment(testDto, correlationId);
        assertNotNull(verified);
        assertEquals("VERIFIED", verified.paymentStatus());

        // Step 3: Verify Kafka send interaction happened
        verify(producerTemplate, atLeastOnce()).sendBodyAndHeaders(eq("direct:sendToodyssey"),
                any(),
                argThat((Map<String, Object> headers) ->
                        "flow-corr-id".equals(headers.get("correlationId")) &&
                        headers.containsKey("eventType")
                )
        );
    }
}
