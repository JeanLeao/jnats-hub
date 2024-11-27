import { connect, StringCodec } from 'nats';

/**
 * Cria um cliente NATS com funcionalidades de concatenação, reconexão, saúde e manipulação de requisições.
 * 
 * @param {Object} [config={}] - Configurações para o serviço NATS.
 * @param {string} [config.servers='nats://localhost:4222'] - URL do servidor NATS.
 * @param {string} [config.prefix='service'] - Prefixo para os nomes de subject.
 * @param {number} [config.timeout=3000] - Tempo de timeout para conexões.
 * @param {number} [config.maxRetries=4] - Número máximo de tentativas de conexão.
 * @param {number[]} [config.retryDelays=[1000, 2000, 5000, 10000]] - Intervalos entre tentativas de conexão.
 * @param {string} [config.serviceName='NATS Service'] - Nome do serviço.
 * @param {number} [config.pingInterval=10000] - Intervalo de ping para verificações de saúde.
 * @param {number} [config.maxPingRetries=3] - Número máximo de tentativas de ping antes de considerar o serviço como falho.
 * @param {number} [config.pingTimeout=2000] - Tempo limite para o ping.
 * @param {Object} [config.services={}] - Configurações dos serviços disponíveis.
 * @param {Function} [config.onBeforeConnect] - Função a ser executada antes da conexão.
 * @param {Function} [config.onAfterConnect] - Função a ser executada após a conexão.
 * @param {Function} [config.onConnect] - Função a ser executada ao se conectar ao NATS.
 * @param {Function} [config.onDisconnect] - Função a ser executada ao se desconectar do NATS.
 * @param {Function} [config.onError] - Função a ser executada em caso de erro.
 * @param {Function} [config.onReconnect] - Função a ser executada quando uma reconexão for bem-sucedida.
 * @param {boolean} [config.reconnect=true] - Se a reconexão automática deve ser ativada.
 * @param {number} [config.maxReconnectAttempts=10] - Número máximo de tentativas de reconexão.
 * @param {number} [config.reconnectTimeWait=2000] - Tempo de espera entre tentativas de reconexão.
 * @param {Function} [config.reconnectDelayHandler] - Função que define o intervalo entre tentativas de reconexão.
 * @returns {Object} Objeto contendo a API pública para manipular conexões, registros e requisições.
 */
function NatsService(config = {}) {
  const defaultConfig = {
    servers: config.servers || 'nats://localhost:4222',
    prefix: config.prefix || 'service',
    timeout: config.timeout || 3000,
    maxRetries: config.maxRetries || 4,
    retryDelays: config.retryDelays || [1000, 2000, 5000, 10000],
    serviceName: config.serviceName || 'NATS Service',
    pingInterval: config.pingInterval || 10000,
    maxPingRetries: config.maxPingRetries || 3,
    pingTimeout: config.pingTimeout || 2000,
    services: config.services || {},
    onBeforeConnect: config.onBeforeConnect,
    onAfterConnect: config.onAfterConnect,
    onConnect: config.onConnect || (() => {}),
    onDisconnect: config.onDisconnect || (() => {}),
    onError: config.onError || (() => {}),
    onReconnect: config.onReconnect || (() => {}),
    reconnect: true,
    maxReconnectAttempts: config.maxReconnectAttempts || 10,
    reconnectTimeWait: config.reconnectTimeWait || 2000,
    reconnectDelayHandler: (attempts) => 
      Math.min(1000 * Math.pow(2, attempts), 30000),
  };

  let connection = null;
  let isConnected = false;
  let healthCheckInterval = null;
  let lastPingTime = null;
  let consecutiveFailures = 0;
  const codec = StringCodec();
  const subscriptions = new Map();
  const handlers = new Map();

  /**
   * Trata os erros ocorridos durante as requisições ou ações no serviço NATS.
   * 
   * @param {Object} error - O erro ocorrido.
   * @param {string} serviceName - Nome do serviço relacionado ao erro.
   * @param {string} action - A ação que causou o erro.
   * @throws {Error} Lança erros customizados baseados no código de erro.
   */
  const handleError = (error, serviceName, action) => {
    if (error.code === 'TIMEOUT') {
      throw new Error(`Timeout: Nenhum serviço está escutando o subject "${serviceName}.${action}"`);
    } else if (error.code === 'CONNECTION_REFUSED') {
      throw new Error('Não foi possível conectar ao servidor NATS. Verifique se ele está rodando.');
    }
    throw error;
  };

  /**
   * Função de verificação de saúde do serviço.
   * 
   * @param {string} [serviceName] - Nome do serviço a ser verificado.
   * @returns {Promise<boolean>} Retorna true se o serviço estiver saudável, caso contrário, false.
   */
  const healthCheck = async (serviceName) => {
    if (!isConnected) return false;

    try {
      const service = serviceName && defaultConfig.services[serviceName];
      const subject = service 
        ? `${service.prefix}.health`
        : `${defaultConfig.prefix}.health`;
      
      const response = await connection.request(
        subject,
        codec.encode('ping'),
        { timeout: defaultConfig.pingTimeout }
      );
      
      const isPong = codec.decode(response.data) === 'pong';
      
      if (isPong) {
        if (consecutiveFailures > 0) {
          console.log('[HealthCheck] PING bem-sucedido após falhas:', {
            atual: consecutiveFailures,
            maximo: defaultConfig.maxPingRetries
          });
        }
        lastPingTime = Date.now();
        consecutiveFailures = 0;
      } else {
        consecutiveFailures++;
      }

      return consecutiveFailures < defaultConfig.maxPingRetries;
    } catch (error) {
      consecutiveFailures++;
      return false;
    }
  };

  /**
   * Inicia o intervalo de verificações de saúde do serviço.
   * 
   * @param {number} [intervalMs=1000] - Intervalo em milissegundos entre as verificações de saúde.
   */
  const startHealthCheck = (intervalMs = 1000) => {
    if (healthCheckInterval) clearInterval(healthCheckInterval);

    healthCheckInterval = setInterval(async () => {
      try {
        const natsHealth = connection.isClosed();
        if (natsHealth) {
          console.warn('Servidor NATS não está saudável, tentando reconectar...');
          await reconnect();
          return;
        }

        for (const [serviceName, serviceConfig] of Object.entries(defaultConfig.services)) {
          const isHealthy = await healthCheck(serviceName);
          if (!isHealthy && serviceConfig.onUnhealthy) {
            serviceConfig.onUnhealthy(serviceName);
          }
        }
      } catch (error) {
        console.error('Erro no health check:', error);
        await reconnect();
      }
    }, intervalMs);
  };

  /**
   * Realiza a reconexão ao servidor NATS.
   * 
   * @returns {Promise<void>} Retorna uma promise que é resolvida quando a reconexão é concluída.
   */
  const reconnect = async () => {
    try {
      defaultConfig.onReconnect();
      await disconnect();
      await initialize();
    } catch (error) {
      console.error('Erro ao reconectar:', error);
    }
  };

  /**
   * Configura o subscription para health check.
   * 
   * @returns {Promise<void>} Retorna uma promise que é resolvida após configurar o subscription de health check.
   */
  const setupHealthCheck = async () => {
    const subject = `${defaultConfig.prefix}.health`;
    const subscription = connection.subscribe(subject);
    subscriptions.set('health', subscription);

    (async () => {
      for await (const message of subscription) {
        const request = codec.decode(message.data);
        if (request === 'ping') {
          message.respond(codec.encode('pong'));
        }
      }
    })().catch(error => console.error('Erro no health check handler:', error));
  };

  /**
   * Registra os handlers de ação.
   * 
   * @returns {Promise<void>} Retorna uma promise que é resolvida após registrar todos os handlers.
   */
  const registerHandlers = async () => {
    for (const [action, handler] of handlers) {
      const subject = `${defaultConfig.prefix}.${action}`;
      const subscription = connection.subscribe(subject);
      subscriptions.set(action, subscription);

      (async () => {
        for await (const message of subscription) {
          try {
            const payload = JSON.parse(codec.decode(message.data));
            const result = await handler(payload);
            message.respond(codec.encode(JSON.stringify(result)));
          } catch (error) {
            message.respond(codec.encode(JSON.stringify({ error: error.message })));
          }
        }
      })().catch(error => console.error(`Erro no handler de ${action}:`, error));
    }
  };

  // API Pública
  return {
    /**
     * Inicializa a conexão NATS e o serviço.
     * 
     * @returns {Promise<boolean>} Retorna true se a inicialização for bem-sucedida, caso contrário, false.
     */
    async initialize() {
      let retryCount = 0;

      while (retryCount < defaultConfig.maxRetries) {
        try {
          if (defaultConfig.onBeforeConnect) {
            await defaultConfig.onBeforeConnect();
          }

          connection = await connect({
            servers: defaultConfig.servers,
            timeout: defaultConfig.timeout,
            reconnect: defaultConfig.reconnect,
            maxReconnectAttempts: -1,
            reconnectTimeWait: defaultConfig.reconnectTimeWait,
            reconnectDelayHandler: () => defaultConfig.reconnectDelayHandler(consecutiveFailures)
          });

          isConnected = true;
          defaultConfig.onConnect();

          await setupHealthCheck();
          await registerHandlers();
          startHealthCheck();

          if (defaultConfig.onAfterConnect) {
            await defaultConfig.onAfterConnect();
          }

          console.log('✅ Serviço NATS iniciado com sucesso');
          return true;
        } catch (error) {
          retryCount++;
          console.error(`❌ Tentativa ${retryCount}/${defaultConfig.maxRetries} falhou:`, error.message);
          
          if (retryCount === defaultConfig.maxRetries) {
            throw error;
          }
          await new Promise(resolve => 
            setTimeout(resolve, defaultConfig.retryDelays[retryCount - 1])
          );
        }
      }
    },

    /**
     * Registra um handler para uma ação específica.
     * 
     * @param {string} action - A ação a ser registrada.
     * @param {Function} handler - A função que será chamada para a ação.
     */
    registerHandler(action, handler) {
      handlers.set(action, handler);
      if (isConnected) {
        registerHandlers();
      }
    },

    /**
     * Realiza uma requisição ao serviço NATS.
     * 
     * @param {string} serviceName - Nome do serviço a ser requisitado.
     * @param {string} action - A ação do serviço.
     * @param {Object} payload - Dados a serem enviados na requisição.
     * @param {Object} [options={}] - Opções adicionais para a requisição.
     * @returns {Promise<Object>} A resposta do serviço.
     */
    async request(serviceName, action, payload, options = {}) {
      if (!isConnected) throw new Error('Cliente NATS não está conectado');
      
      const service = defaultConfig.services[serviceName];
      if (!service) throw new Error(`Serviço "${serviceName}" não configurado`);

      const subject = `${service.prefix}.${action}`;
      const config = {
        maxRetries: options.maxRetries || service.maxRetries || 3,
        baseTimeout: options.baseTimeout || service.baseTimeout || 20000,
        timeoutIncrement: options.timeoutIncrement || service.timeoutIncrement || 10000,
      };

      for (let attempt = 1; attempt <= config.maxRetries; attempt++) {
        try {
          const timeout = config.baseTimeout + (attempt - 1) * config.timeoutIncrement;
          const response = await connection.request(
            subject,
            codec.encode(JSON.stringify(payload)),
            { timeout }
          );

          return JSON.parse(codec.decode(response.data));
        } catch (error) {
          if (attempt === config.maxRetries) {
            handleError(error, serviceName, action);
          }
          await new Promise(resolve => setTimeout(resolve, 2000));
        }
      }
    },

    /**
     * Desconecta do servidor NATS e limpa todos os subscriptions.
     * 
     * @returns {Promise<void>} Retorna uma promise que é resolvida quando a desconexão é concluída.
     */
    async disconnect() {
      if (healthCheckInterval) {
        clearInterval(healthCheckInterval);
        healthCheckInterval = null;
      }

      if (connection && isConnected) {
        for (const subscription of subscriptions.values()) {
          subscription.unsubscribe();
        }
        subscriptions.clear();

        try {
          await connection.drain();
          await connection.close();
        } finally {
          isConnected = false;
          connection = null;
          lastPingTime = null;
          consecutiveFailures = 0;
        }
      }
    },

    /**
     * Retorna as estatísticas de conexão atuais.
     * 
     * @returns {Object|null} Um objeto com as estatísticas de conexão ou null se não estiver conectado.
     */
    getConnectionStats() {
      if (!connection || !isConnected) return null;

      return {
        serverInfo: connection.info,
        pingInterval: defaultConfig.pingInterval,
        lastPingTime,
        consecutiveFailures,
        isConnected,
        uptime: lastPingTime ? Date.now() - lastPingTime : 0
      };
    }
  };
}

export default NatsService;
